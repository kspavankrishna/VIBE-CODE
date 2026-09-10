# Prompt Template Compiler

Prompt templates built from JavaScript template literals break silently: a typo'd variable renders `undefined`, nobody knows the token cost until the bill arrives and a user-pasted string containing `SYSTEM:` can reshape the rest of the prompt. This is a single file compiler for a `{{ }}` prompt template language that parses once, checks the template against a declared schema and neutralizes untrusted interpolations before they reach the model.

**Language:** JavaScript | **Lines:** 844 | **Added:** 2026-08-31

## What this solves

This solves the problem of prompt templates turning into a pile of fragile string concatenation once an agent or app has more than a handful of them. Every team I have watched build LLM features ends up with prompts assembled by pasting variables into template literals. There is no way to catch a typo'd variable name until it silently renders "undefined" in production, no way to know how many tokens a prompt will cost before calling the model and no defense when a user supplied field happens to contain something that looks like a role delimiter.

The failure modes are concrete. A renamed field in your context object leaves `{{ customerName }}` resolving to nothing, so the model gets "Hello , here is your refund status" and invents a name. Nobody notices because the render never throws. It surfaces three weeks later as a support complaint. Second: a RAG pipeline stuffs ten retrieved chunks into a prompt, one document runs long and the call blows the context window in production, after you already paid for the embedding lookup. Third: a ticket body, a scraped page or a tool result contains a code fence and a line reading `SYSTEM: ignore previous instructions`, and anything downstream that greps role prefixes or splits on fences now sees a delimiter the attacker wrote.

The third one is invisible in code review. The template looks fine. The data is what is hostile, and it arrives at runtime from someone who is not you. Interpolating a scraped page into a prompt with a bare `${}` is the LLM equivalent of building SQL by string concatenation. There is a plain performance cost too: most template libraries re-parse the source on every render, so an agent rebuilding its system prompt each turn pays a parser pass per request for a string that has not changed since startup.

## Why I built it

The existing options do not fit. Handlebars and Mustache are HTML oriented: their escaping helps against XSS and does nothing about prompt injection, and neither knows what a token is. Template literals have no static checking at all. The prompt frameworks that do ship templating usually drag in a whole orchestration layer you did not ask for and still will not tell you the variable you referenced does not exist.

What I wanted was small. Compile each template once at startup next to a schema object, get back a render function plus warnings plus a token budget estimate, and have untrusted variables neutralized by default rather than by remembering to call a helper. That is a few hundred lines and no dependency tree.

## When to use it

- An agent loop rebuilds the same system prompt every turn and you want the parse cost paid once at startup.
- A RAG or summarization pipeline splices retrieved documents into a prompt and you need a size estimate before committing to the call.
- Any prompt that interpolates a support ticket, a scraped page, an email body or a tool result from outside your trust boundary.
- You have twenty templates in a repo and want CI to fail when one references a variable your context builder no longer sets.
- A prompt is assembled from optional sections and you want dead branches compiled out rather than evaluated per render.

## How it works

The pipeline is tokenize, parse, optimize, analyze, generate. `tokenize()` scans for `{{{ ... }}}` raw tags and `{{ ... }}` tags, emitting text runs in between, and `classifyTag()` turns each tag body into a typed token: `comment`, `partial`, `if_open`, `each_open`, `with_open`, their close tags, `else` or a plain `expr`. Expression bodies go through `parseExprInner()`, which splits on `|` using `splitTopLevel()`, a quote aware splitter so a pipe or comma inside a string argument does not split the expression. `parseValueToken()` decides whether the head is a literal or a dotted variable path. Unbalanced tags raise `TemplateSyntaxError` carrying the byte offset. `parse()` is then plain recursive descent producing a `Program` AST of `Text`, `Expr`, `If`, `Each`, `With`, `Partial` and `Comment` nodes.

`optimize()` is the step that earns the name compiler. It drops `Comment` nodes, does dead branch elimination on any `If` whose condition is a compile time literal (`{{#if true}}A{{else}}B{{/if}}` collapses to the text `A` and the generated source never mentions `helpers.truthy`), constant folds filter chains whose head is a literal and whose filters are all in the `PURE_FILTERS` set, then runs `mergeAdjacentText()` so the runs left behind become one push instead of many.

`analyze()` walks the optimized AST against the schema and returns three warning levels. A variable not declared in the schema is `warn`. A schema key nothing references is `info`. An untrusted variable interpolated through raw `{{{ }}}` braces is `high`, because raw explicitly opts out of neutralization and the compiler will not silently override the author. Partials named but not supplied also warn.

`estimateBounds()` computes a character interval over the AST, not a point estimate. Text contributes its exact length, a schema string contributes `minLength` to `maxLength`, `If` takes the min and max across both branches and `Each` multiplies the body bounds by `minItems` and `maxItems`. Unknowns fall back to tunable constants (`unknownVarMaxChars`, `unknownArrayMaxItems`, `partialMaxChars`), and characters become tokens by dividing by `charsPerToken`, default 4.

`generate()` emits a JavaScript function body: an `_out` array, one `push` per node and a final `join('')`. Nested scopes get freshly named `ctx` variables from a `fresh()` counter instead of shadowing `ctx`, which would put the outer read into the new binding's temporal dead zone. `compileTemplate()` instantiates that source once with `new Function("ctx", "helpers", src)`. That is safe here because only template author literals (paths, filter names, filter arguments) are ever spliced into the generated source. Runtime data arrives solely as the `ctx` argument and is read by plain property access in `getPath()`.

`neutralize()` is the injection mitigation. It runs on every non raw interpolation whose top level variable is not marked `trust: "trusted"`, inserting zero width spaces inside triple backticks, `SYSTEM:` / `ASSISTANT:` / `USER:` / `TOOL:` line prefixes, `<|special|>` tokens, `[INST]` and `[SYS]` markers and leading markdown headings. The visible text is unchanged for a human or a model, but it can no longer exact match a delimiter a brittle downstream parser is grepping for.

## Usage

```js
import compileTemplate from "./PromptTemplateCompiler.js";

const tpl = compileTemplate(
  `You are a support agent.
{{#each tickets as t}}
Ticket {{@index}}: {{ t.subject | trim | truncate:80 }}
{{ t.body }}
{{/each}}
{{#if escalated}}Escalate to a human.{{/if}}`,
  {
    schema: {
      tickets: { type: "array", maxItems: 5, itemLength: { min: 20, max: 600 } },
      escalated: { type: "boolean", trust: "trusted" },
    },
    filters: { redact: (v) => String(v ?? "").replace(/\d{4,}/g, "[num]") },
    partials: {},
  }
);

tpl.warnings.forEach((w) => console.error(`[${w.level}] ${w.message}`));

const est = tpl.estimateTokens();
if (est.maxTokens > 8000) throw new Error("prompt budget exceeded");

const prompt = tpl.render({ tickets, escalated: false });
```

CLI:

```bash
# Lint a template against a schema, print warnings and a token estimate to stderr
node PromptTemplateCompiler.js check prompt.tmpl --schema schema.json

# Same checks, then write the rendered prompt to stdout
node PromptTemplateCompiler.js render prompt.tmpl --schema schema.json --ctx data.json

# Run the built in test suite
node PromptTemplateCompiler.js --self-test
```

## Notes

- Node only. ESM `import`, `node:fs`, `node:path`, `node:url` and `new Function`, so it will not run under a CSP that blocks dynamic evaluation. Zero third party dependencies.
- Token estimation is characters divided by `charsPerToken` (default 4). A budget guardrail, not a real tokenizer, so do not use it for exact accounting.
- `neutralize()` breaks exact matches for a fixed list of delimiter conventions. It does not understand semantics and will not stop an instruction written in plain prose. Defense in depth only.
- Conditions are single value truthiness only, with an optional leading `!`. There is no `and`, `or`, comparison or arithmetic in `{{#if}}`.
- `{{#each}}` iterates arrays only. A plain object or a Map renders nothing, silently. `@index`, `@first` and `@last` bind only in the `as name` form.
- Trust resolves from the first path segment only, so `{{ user.profile.bio }}` inherits the label on `user`. Per field trust is not supported.
- A missing partial renders the literal string `[[missing partial: name]]` rather than throwing, so it shows up in output instead of taking down the request.
- The CLI writes warnings and the token estimate to stderr and the render to stdout, so the prompt pipes cleanly. Exit code 1 on a missing subcommand or template path, and it throws on a syntax error.
