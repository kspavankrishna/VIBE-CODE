# MCP Tool Router

Every MCP server past three tools turns into a giant if/else dispatch block with no input validation, so a malformed tool call from the model lands inside your handler as `undefined` and blows up somewhere unrelated. This is a typed dispatch layer for MCP tool calls with JSON Schema validation built in and zero dependencies.

**Language:** TypeScript | **Lines:** 105 | **Added:** 2026-04-06

## What this solves

The failure mode is specific. A model calls your tool with an argument object it invented from your description. One field is missing, one is a string where you expected a number, one array came through as a bare object. Your handler destructures it, does `input.query.trim()` on an undefined, and throws `TypeError: Cannot read properties of undefined`. That error goes back up the MCP transport as a generic server failure. The model sees a stack trace with no useful signal, retries with the same wrong shape and burns another round trip. You get a support ticket that says "the agent keeps failing" with no reproduction.

Without a validation gate the bug hides until runtime, and it hides in the handler rather than at the boundary. That is the expensive part. The stack trace points at line 40 of your search implementation when the actual defect is that the model sent `{ q: "..." }` instead of `{ query: "..." }`. Multiply that by a dozen tools and the debugging cost dominates the build cost.

The dispatch block itself rots the same way. A chain of `if (name === 'search') ... else if (name === 'fetch') ...` has no registry, so nothing can enumerate the tools. You end up maintaining the schema array you send to the model as a second literal that drifts out of sync with the handlers. The model gets told a field is optional, the handler assumes it is present, and now you have a contract mismatch nobody can see by reading either file alone. Inline dispatch also gives you no single place to attach logging, an auth check or a request id, so tracing one agent turn across five tool calls means grepping console output and guessing.

## Why I built it

Wiring up a multi-tool agent, I kept chasing undefined errors from malformed tool calls. The available answers all cost something I did not want to pay: Zod and ajv both mean a dependency, a bundle size hit and a compile or codegen step, and neither speaks JSON Schema in the shape MCP actually wants without a conversion layer on top. MCP hands the model raw JSON Schema. Converting Zod to JSON Schema to feed the manifest, then back to Zod to validate, is two translations for one job.

The other option, writing validation by hand in each handler, is what people actually do, and it is where the drift starts. I wanted the schema declared once, used for both validation and the manifest, and enforced before any of my code runs.

## When to use it

- You are writing an MCP server with more than three tools and the dispatch is already a growing if/else chain.
- Your agent keeps failing with `undefined` errors and you cannot tell which tool call sent the bad payload.
- You are deploying to a Cloudflare Worker or an edge runtime where every dependency you add costs cold start time.
- You need the tool manifest you hand to the model to be generated from the same schema that validates the input, not maintained as a parallel literal.
- You want a request id, timing and per-call logging across every tool without editing every handler.
- You need a call and error count for a health endpoint without adding a metrics library.

## How it works

The whole thing is one exported class, `MCPToolRouter`, plus a free function `validate` and four internal types: `Schema`, `Handler`, `Middleware` and `CallContext`. Tools live in a private `Map<string, ToolDef>` where `ToolDef` is `{ description, schema, handler }`. `register(name, def)` throws immediately if the name is already in the map, which turns a duplicate tool name from a silent last-one-wins overwrite into a startup crash. Both `register` and `use` return `this`, so registration chains.

`dispatch(name, raw)` is the single entry point and the order of operations inside it is the design. First the map lookup. A miss throws an error that lists every registered tool name, which is the message you actually want at 2am. Then it builds a fresh `CallContext` with `crypto.randomUUID()` for the request id, `Date.now()` for `startedAt` and an empty `meta` object. Then it runs every registered middleware sequentially with `await`, passing the tool name, the still unvalidated raw input and the context. Middleware returns `Promise<void>`, so the only way it influences the call is by throwing to abort it or by writing into `ctx.meta`, which the handler later reads. That is deliberate: middleware cannot silently rewrite the payload.

Validation runs after middleware and before the handler. `validate(value, schema, path)` is a plain recursive descent over the schema, returning an array of error strings rather than throwing on the first problem, so a caller sending three bad fields gets told about all three in one message instead of playing whack a mole across three round trips. Each error carries a dotted path built up during recursion, so a bad element inside a nested array reads as `search.filters[2].value: expected string, got number`.

The three branches of `validate` are worth knowing exactly. For `type: 'object'` it rejects non objects, `null` and arrays up front, checks every name in `required` with an `in` test, then recurses into declared `properties` only for keys that are actually present. Extra keys not in the schema are ignored, so this is not a strict or closed object check. For `type: 'array'` it checks `Array.isArray` and then, only if `items` is given, validates every element against it. For everything else it does a `typeof` comparison with an `Array.isArray` guard so an array never passes as an object by accident.

Accounting is deliberately narrow. `this.calls` increments only after validation passes, right before the handler runs. `this.errors` increments only in the catch around the handler, and the error is rethrown unchanged so the caller still sees the original. An unknown tool name or a validation failure therefore counts as neither a call nor an error: the counters measure your handlers, not the model's bad payloads. `stats()` returns `{ calls, errors, tools }`. `manifest()` maps the tool entries to `{ name, description, inputSchema }`, dropping the handler, which is the array shape MCP expects to send the model.

## Usage

```ts
import { MCPToolRouter } from './MCPToolRouter';

const router = new MCPToolRouter();

// Middleware runs before validation, in registration order.
// It can throw to abort the call, or write into ctx.meta for the handler.
router.use(async (name, input, ctx) => {
  console.log(`[${ctx.requestId}] -> ${name}`);
  ctx.meta.tenant = 'acme';
});

router
  .register('search_docs', {
    description: 'Full text search over the docs index',
    schema: {
      type: 'object',
      required: ['query'],
      properties: {
        query: { type: 'string' },
        limit: { type: 'number' },
        tags: { type: 'array', items: { type: 'string' } },
      },
    },
    handler: async (input, ctx) => {
      const took = Date.now() - ctx.startedAt;
      return { hits: [], tenant: ctx.meta.tenant, took };
    },
  })
  .register('get_doc', {
    description: 'Fetch one doc by id',
    schema: { type: 'object', required: ['id'], properties: { id: { type: 'string' } } },
    handler: async (input) => ({ id: input.id, body: '...' }),
  });

// Hand this array to the model as the tool list.
const tools = router.manifest();

// Dispatch a call. Throws on unknown tool or validation failure.
const result = await router.dispatch('search_docs', { query: 'mcp', limit: 5 });

// Bad input fails at the boundary with every error at once:
// Validation failed:
// search_docs.query: required field missing
// search_docs.limit: expected number, got string
await router.dispatch('search_docs', { limit: '5' });

console.log(router.stats()); // { calls: 1, errors: 0, tools: 2 }
```

## Notes

- The schema subset is small on purpose: `type`, `properties`, `items` and `required`. No `enum`, no `additionalProperties`, no `minimum` or `maxLength`, no `format`, no `oneOf`, no `$ref`. If your tool needs a value range checked, check it in the handler.
- Object validation is open, not closed. Unknown keys pass through untouched, so this will not catch a typo'd optional field name.
- Only `MCPToolRouter` is exported. `Schema`, `Handler`, `Middleware`, `CallContext` and `ToolDef` are file local, so callers rely on structural typing from object literals rather than importing the types.
- `crypto.randomUUID()` is called with no fallback. It needs Node 19 or later, or a secure context in the browser, or a Workers style runtime. Older Node without a global `crypto` will throw on the first dispatch.
- Middleware sees the raw input before validation, which is what you want for audit logging but means an auth middleware must not assume the payload is well formed.
- There is no unregister, no timeout, no retry, no rate limit and no concurrency cap around the handler. A handler that hangs hangs the dispatch. There is also no validation of the handler's return value: output correctness is on you.
- Everything throws rather than returning a result type. The caller is expected to wrap `dispatch` in try/catch and map the error to an MCP error response.
