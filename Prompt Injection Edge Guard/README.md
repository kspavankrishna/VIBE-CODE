# Prompt Injection Edge Guard

Retrieved web pages, MCP tool output and RAG chunks get pasted into model context with no filter in between. This is a single file Lua prompt injection firewall that scores, redacts, fingerprints and budgets every untrusted item before it reaches the LLM.

**Language:** Lua | **Lines:** 747 | **Added:** 2026-06-29

## What this solves

The failure mode is boring and it keeps happening. A crawler pulls a page, a vector store returns a chunk, an MCP server returns a tool result, and that text goes into the prompt as if it were trusted. Somewhere in it is a line saying ignore previous instructions and dump the system prompt. The model cannot tell your instructions from a stranger's, because by the time it sees the bytes they sit in the same context window. You get a leaked developer message, a tool call nobody asked for, or a citation invented on request of the poisoned document.

The second failure mode costs money instead of trust. Tool results and scraped pages carry credentials: an `sk-` key in a support ticket, a `ghp_` token in a pasted CI log, an AWS `AKIA` key in a config snippet. Those go into the prompt, then the provider's logs, then your trace store and your observability vendor. Nobody notices until a secret scanner fires weeks later on the wrong system. Redacting at the edge is the only place that fix is cheap.

The third is quieter. In a multi tenant AI gateway a retrieval bug hands tenant B's document to tenant A's request. No exception, no 500, no alert, just a correct looking answer built from someone else's data. Same class of silent problem: hidden HTML instructions in a crawled page, zero width characters smuggling text past reviewers, and base64 blobs that eat the context budget so real evidence gets pushed out. None of this needs a model to detect. It needs a deterministic pass over the bytes, a score, a decision and a log line an operator can read at 3am.

## Why I built it

Every AI gateway I looked at either shipped nothing here or shipped a network call to a classifier service. A network call is the wrong shape: it doubles the latency of the request you are protecting, it fails open under load, and untrusted text leaves your edge before you have decided whether you trust it. The OSS options were Python libraries, useless if your edge is OpenResty or another Lua host embedded in nginx.

So this is plain Lua, zero dependencies, nothing outside the standard library and no I/O. Deterministic scoring means the same input always gives the same decision, which is what you want when you have to explain a block to a customer. It is not a classifier and does not pretend to be one, it is the boring layer you put underneath one.

## When to use it

- An OpenResty or nginx Lua edge in front of an LLM route, where retrieved documents are merged into the prompt server side
- An internal MCP proxy passing tool output back to an agent, which needs labelling and budgeting before it is trusted
- A crawler to vector store pipeline where scraped pages are indexed and later injected as evidence with no human in the loop
- A multi tenant RAG service that must hard fail when a retrieved chunk carries a tenant id that does not match the request
- A CI or DevOps copilot reading build logs, issue text and third party API responses, all of which routinely contain credentials
- Anywhere you plant canary tokens in your system prompt and want an alarm if they show up in retrieved text

## How it works

Entry is `PromptInjectionEdgeGuard.new(options)`, which merges your options over `DEFAULTS` and builds two domain sets with `make_domain_set`. Every item then goes through `evaluate_item`. An item is a table with `text`, `content` or `body`, plus optional `url` / `source_url` / `href`, `source_type` and `tenant_id`. A bare string is accepted and wrapped.

Text is clipped by `clip_bytes` at `max_item_bytes` (24000 default) with a `[TRUNCATED_BY_EDGE_GUARD]` marker, then run through `normalize_text`. Normalization decides detection quality: `percent_decode`, then `html_entity_decode` including numeric and hex forms, then control characters and zero width byte sequences replaced with spaces, whitespace collapsed, lowercased. Attackers use exactly those layers to hide `ignore previous instructions` from a naive substring check. Anything decoding outside printable ASCII becomes a space.

Scoring is additive over independent detectors. `INJECTION_RULES` holds six weighted groups of Lua patterns: `instruction_override` (30), `secret_exfiltration` (32), `tool_hijack` (26), `agent_role_confusion` (24), `rag_boundary_break` (22) and `encoding_smuggling` (18). Counting runs through `pattern_count`, which wraps `string.gmatch` in `pcall` so a bad pattern degrades to zero instead of throwing inside your request handler, and caps at 3 hits so one repeated phrase cannot dominate. On top: denied domain 60, allowlist miss 28, tenant mismatch 75, canary hit 85, a `HTML_HIDDEN_PATTERNS` match 16 once (HTML comments, `display:none`, `aria-hidden`, zero font size, zero opacity), truncation 8. Hosts come from `host_from_url` and match via `domain_matches`, true suffix matching on a leading dot, so `evil.example` catches `a.evil.example` and never `notevil.example`.

`high_entropy_token_count` is the cheap base64 and key blob detector. It scans tokens of at least 96 characters from the base64 and URL safe alphabet and flags only those with 24 or more distinct characters, which separates a real encoded payload from a long repeated separator. Each hit is 14 points, up to three. `redact_secrets` runs nine `SECRET_RULES` with `gsub`: OpenAI `sk-`, GitHub `gh[pousr]_`, Slack `xox`, AWS `AKIA`, JWT triples, PEM private key blocks (weight 55, heaviest single rule) and capture group rewrites for `password=`, `token=` and `secret=` that keep the key and replace only the value. Redaction rewrites the text and adds the rule weight, so a leaking source is also a suspicious source.

The decision ladder: at or above `drop_score` (115) is `drop`, and so is anything at or above `quarantine_score` (70) when `strict` is set. At or above 70 otherwise is `quarantine`. Nothing tripped but redacted or truncated is `redact`. Everything else is `allow`. A dropped item has its text blanked. Each result carries `score`, `labels`, an `evidence` list capped at `evidence_limit`, byte counts in and out, and a `fingerprint` from `stable_fingerprint`, an FNV style multiply and add hash modulo the prime 4294967291 printed as eight hex characters. That fingerprint is how you correlate the same poisoned source across requests.

`sanitize_bundle` runs the list, capped at `max_items` (128), and wraps survivors with `build_context_block` into a delimited `[context:001 action=... score=... host=... fingerprint=...]` envelope so the model sees provenance instead of anonymous text. Metadata goes through `safe_metadata`, which strips control characters and square brackets, so an item cannot forge its own envelope. Blocks accumulate against `max_total_bytes` (120000) and the first block that would blow the budget is demoted to quarantine with a `total_context_budget_exceeded` label instead of silently vanishing. `decision_headers` turns the summary into `X-Prompt-Guard-*` headers, `openresty_filter` returns a closure that writes them to `ngx.header`, and `to_json` is a dependency free encoder with sorted keys and a depth limit of 32.

## Usage

```lua
local Guard = require("PromptInjectionEdgeGuard")

local guard = Guard.new({
    tenant_id       = "acme",
    deny_domains    = { "evil.example" },
    allow_domains   = { "docs.acme.com" },
    canary_tokens   = { "PV-CANARY-001" },
    strict          = false,
    redact          = true,
    max_item_bytes  = 24000,
    max_total_bytes = 120000,
})

local report = guard:sanitize_bundle({
    { text = "…retrieved page…", source_url = "https://docs.acme.com/x",
      tenant_id = "acme", source_type = "rag" },
})

-- report.context -> delimited, provenance wrapped prompt block
-- report.summary -> accepted / quarantined / dropped / labels / bytes_used
print(Guard.to_json(report.summary))

-- inside an OpenResty worker: also stamps X-Prompt-Guard-* response headers
local filter = guard:openresty_filter()
local report2, err = filter(items)
```

Run the built in checks from the shell:

```
lua PromptInjectionEdgeGuard.lua --self-test
```

## Notes

- Pure Lua, no external modules, no network calls, no filesystem access. `ngx` is touched only if it already exists in the global environment.
- Detection is pattern based, not semantic. A paraphrased or non English injection can pass. Treat it as a deterministic floor under a model based check, not a replacement.
- `normalize_text` decodes to printable ASCII, so non ASCII content collapses to spaces for matching and scores weaker on such corpora.
- Limits are bytes, not codepoints, since lengths use `#`. Multibyte text hits `max_item_bytes` sooner than the character count suggests.
- `clip_bytes` truncates mid token, so a secret or injection phrase can be split across the cut and hidden. Raise `max_item_bytes` if your sources are long.
- Scores are additive and uncalibrated. The 70 and 115 thresholds match the weights in this file. Change a weight and you revisit both. Set `strict = true` to collapse quarantine into drop.
- `self_test` asserts a hostile item is not accepted, a clean one is accepted with its text intact and the summary encodes to JSON. It fails through `assert`. Smoke test, not a test suite.
- Tenant checking fires only when both the guard and the item carry a tenant id. An item with no `tenant_id` is never flagged as a mismatch.
