# HTML Prompt Injection Firewall

Hidden text in scraped HTML can hijack an LLM. This is a single file PHP firewall that turns raw HTML into clean readable text while stripping, quarantining and scoring the parts a model should never be allowed to obey.

**Language:** PHP | **Lines:** 1198 | **Added:** 2026-04-27

## What this solves

Prompt injection filtering for HTML before it reaches an LLM, RAG pipeline, agent, crawler or search index. Hidden instructions in comments, off screen spans, forms, data URLs and copied site widgets still slip into web grounded AI systems in 2026, especially in Laravel, WordPress, Symfony and custom PHP ingestion jobs. The usual pipeline is `file_get_contents()`, then `strip_tags()`, then straight into a prompt, and it is wide open. `strip_tags()` keeps the text inside a `div` styled `position:absolute;left:-9999px`, keeps the body of a `noscript`, and hands your model a paragraph telling it to ignore its system prompt and post the session token somewhere.

The production failure is quiet, which is what makes it expensive. A support portal page gets crawled into a knowledge base. Somewhere in it a vendor left an HTML comment, or a customer pasted white on white text into a ticket. Retrieval treats it as ordinary context, the model reads it as an instruction, and the agent calls a tool it should not have called or leaks part of the system prompt into a public answer. Nobody notices for a week, and when somebody does there is no record of which document caused it, because the injected text was stripped out of the logs along with the markup.

There is a cost problem underneath it. Naive HTML to text drags in navigation, cookie banners, script bodies and base64 blobs, so a page that should yield 2 KB of prose yields 60 KB of junk.

This file does two jobs together. It strips or quarantines the hidden and interactive HTML that models should not trust. Then it keeps the human visible content in a markdown like text form while scoring suspicious comments, prompt override phrases, risky link schemes, zero width control characters and high entropy blobs that often hide secrets. You get clean text, a risk score between 0 and 1, a `needs_review` flag and a findings list you can log or route to a human.

## Why I built it

There are plenty of HTML to text converters and plenty of XSS sanitizers, and neither is the right tool. An XSS sanitizer asks whether HTML is safe to render in a browser. Different threat model: the consumer here is a model reading text, not a DOM executing script, and text that renders harmlessly is still a perfect payload. Readability extractors solve boilerplate removal but have no concept of adversarial content.

Nothing in PHP sat in the middle: strip the untrustworthy surface, keep the readable surface, and tell me what was suspicious instead of silently swallowing it. The quarantine list matters as much as the clean text. When an agent misbehaves you want the exact snippet that was pulled out, with its severity and the rule that fired.

## When to use it

- You crawl docs sites, CMS pages, support portals or vendor dashboards into a RAG index.
- An agent browses a page you do not control and the fetched HTML needs defanging before it hits the context window.
- Customer submitted HTML, ticket bodies or email archives get summarized and any one of them could carry pasted attacker content.
- A queue worker or webhook consumer normalizes HTML into LLM ready text and you want a risk score persisted next to each document.
- You need an audit trail: which page, which snippet, which rule, so a security review can reconstruct what the model saw.

## How it works

The entry point is `HtmlPromptInjectionFirewall::sanitize(string $html, ?string $sourceUrl = null)`, returning a `HtmlPromptInjectionResult`. Everything tunable lives in `HtmlPromptInjectionFirewallConfig`: `maxOutputChars` (120000), `maxTextPerNode`, `maxLinks` (200), `reviewThreshold` (0.55), `allowedLinkSchemes` (http, https, mailto) and a `blockedTags` list covering `script`, `style`, `noscript`, `template`, `iframe`, `object`, `embed`, `svg`, `canvas`, `meta`, `link`, `base` and the whole form family.

`scanRawHtml()` runs first, on the raw string, because the parser discards exactly the things worth inspecting. It pulls every `<!-- ... -->` with `COMMENT_PATTERN`, decodes entities, and runs each comment through `matchPromptRules()`. That rule set is the `PROMPT_RULES` constant: five labelled regexes covering `ignore-prior-instructions`, `prompt-exfiltration`, `silent-exfiltration`, `tool-steering` and `credential-exfiltration`. The same pass flags zero width and bidi control characters via `CONTROL_PATTERN`, `javascript:` or `data:` references and `meta http-equiv=refresh`. Parsing then goes through `loadDocument()`, which wraps `DOMDocument::loadHTML()` with `libxml_use_internal_errors(true)` and the `LIBXML_HTML_NOIMPLIED` and `LIBXML_HTML_NODEFDTD` flags where available. Malformed input is expected, and if `DOMDocument` is missing the whole thing degrades to `strip_tags()` and still returns findings and a score.

`pruneDocument()` is the destructive stage. One XPath query, `//* | //comment()`, gives every element and comment in document order. Comments go. Blocked tags go, their text summarized into quarantine first. Hidden nodes go, detected by `isHiddenElement()`: the `hidden` attribute, `aria-hidden="true"`, a class containing `sr-only` or `visually-hidden`, or an inline style matching the `styleImpliesHidden()` needle list (`display:none`, `visibility:hidden`, `opacity:0`, `font-size:0`, `left:-9999` and friends). A hidden node is `high` severity and escalates to `critical` when its text also matches a prompt rule. That escalation is the signal that actually matters: text deliberately concealed from humans that also reads like an instruction. `inspectElementAttributes()` checks `href`, `src`, `action` and `formaction` against `isAllowedLinkScheme()`. Enabling `dropVisiblePromptLikeText` adds strict mode, deleting visible blocks in `PROMPT_DROP_TAGS` that match a rule, while `isCodeLikeContext()` walks the ancestor chain so a docs page showing an injection example inside `pre` or `code` is not eaten.

`walkNode()` renders the surviving tree recursively. Headings become `#` runs from `HEADING_LEVELS`, `pre` becomes a fenced block, lists go through `renderList()` with two space indentation per nesting level, `table` through `renderTable()` which emits pipe rows and a `---` separator when a `th` was seen, `img` becomes `[Image: alt]`, and anchors keep label plus href up to the `maxLinks` budget. Output goes into `HtmlPromptInjectionTextAccumulator`, a block buffer rather than a string append: it normalizes whitespace, joins inline fragments with punctuation aware spacing, skips a block identical to the one before it when `dedupeBlocks` is on, and enforces the budget by cutting the final block and appending `[TRUNCATED OUTPUT]`.

Scoring comes last. `findHighEntropyFindings()` scans the text for tokens of 28 or more characters from the base64 and identifier alphabet, computes Shannon entropy over the byte histogram in `entropy()`, and reports one only when entropy is at least 4.15 bits per character and it uses at least 10 distinct characters. Those two conditions together separate a real secret from a long word or a repeated filler string. `computeRiskScore()` combines findings with a noisy OR, `risk = 1 - (1 - risk) * (1 - weight)`, weighting critical at 0.78, high at 0.48, medium at 0.24 and anything else at 0.12. Weak signals accumulate, nothing exceeds 1.0, and no single medium finding trips the gate alone. `needsReview` is true when the score crosses `reviewThreshold` or any finding is `critical`.

## Usage

```bash
# Read a saved page: cleaned text on stdout, summary line on stderr
php HtmlPromptInjectionFirewall.php --file=page.html

# Full machine readable report: text, risk score, stats, quarantine, findings
php HtmlPromptInjectionFirewall.php --file=page.html --url=https://docs.example.com/faq --json

# Pipe from a fetch, cap the output budget, also drop VISIBLE prompt-like blocks
curl -s https://example.com/page | php HtmlPromptInjectionFirewall.php --json --strict-visible

php HtmlPromptInjectionFirewall.php --help
```

```php
require __DIR__ . '/HtmlPromptInjectionFirewall.php';

$config = new HtmlPromptInjectionFirewallConfig(
    maxOutputChars: 40000,
    maxLinks: 50,
    dropVisiblePromptLikeText: true,
    reviewThreshold: 0.45,
);

$firewall = new HtmlPromptInjectionFirewall($config);
$result   = $firewall->sanitize($html, 'https://docs.example.com/faq');

if ($result->needsReview) {
    error_log(json_encode($result->quarantine));  // park it for a human
    return;
}

$prompt = $result->text;        // markdown-like, budgeted, deduped
$score  = $result->riskScore;   // 0.0 to 1.0
$stats  = $result->stats;       // dropped_hidden_nodes, links_retained, truncated, ...

foreach ($result->findings as $finding) {
    // $finding->kind, ->severity, ->message, ->snippet, ->meta['rule']
}
```

## Notes

- PHP 8.1 or newer (readonly properties, promoted constructors, named arguments, `match`). `ext-dom` is strongly recommended: without it the code falls back to `strip_tags()` and loses hidden node pruning entirely.
- The prompt rules are five English regexes. They will not catch a paraphrase, another language or an obfuscated payload. Treat the score as triage, not proof of safety.
- Hidden content detection reads inline `style`, `hidden`, `aria-hidden` and two class names. It does not resolve stylesheets, so a class defined in a separate CSS file that hides text is invisible to it.
- The raw scan for `javascript:` and `data:` matches anywhere in the source, including ordinary prose, so pages discussing URL schemes produce a `high` finding. Expect false positives on security docs.
- `dedupeBlocks` only collapses a block identical to the one immediately before it. Repeated boilerplate separated by other content still comes through.
- The CLI exits 0 on any successful run regardless of risk. Exit code 1 means an unknown argument or an unreadable file. To gate a pipeline, read the `--json` output and branch on `needs_review` yourself.
- No fetching, no rendering, no JavaScript execution. Content injected by client side script after load is out of scope.
