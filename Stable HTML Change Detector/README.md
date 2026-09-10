# Stable HTML Change Detector

Hashing raw HTML to detect website changes gives you a false positive on every page load, because hydration IDs, nonces, session tokens and timestamp banners rotate on every render. This TypeScript file fingerprints the semantic content of a page instead, so an alert only fires when the text people actually read has changed.

**Language:** TypeScript | **Lines:** 435 | **Added:** 2026-04-27

## What this solves

This solves flaky website change detection for teams that monitor public HTML pages and only want alerts when the page meaning actually changed. The naive version is three lines: fetch the page, SHA-256 the body, compare against the last hash. It works for about a week. Then the site ships a React upgrade, a CDN stamps a request ID into the footer or the framework writes a fresh `nonce` on every script tag, and the monitor screams every fifteen minutes forever.

The failure mode is not a loud break. It is noise. Someone mutes the channel. Two months later a compliance page changes a fee schedule, the alert fires like the previous four thousand and nobody looks. The cost is an on call engineer who stopped trusting the signal, and the change you were paid to catch going through unseen.

The opposite mistake is just as common. Teams burned by raw hashing overcorrect and strip the page to `innerText`, which throws away structure. A link whose href changed to a phishing domain then looks identical, because the anchor text did not move.

This file sits between the two extremes. It keeps structure, discards the parts of structure that machines regenerate and emits a per block diff with stable keys, so the alert can name which heading, which list item or which link moved.

## Why I built it

A lot of April 2026 monitoring work still breaks on modern frontend churn. Hosted change detection services either charge per page and per check or treat the DOM as opaque text, and the open source scripts I found were either a `diff` wrapper around `html2text` or a headless browser stack you have to babysit. Nothing in between was small enough to read in one sitting.

So this is plain TypeScript with only Node built ins: `node:crypto`, `node:fs/promises` and `node:url`. No parser dependency, no browser, no service. Fork it, read the regexes, add your own noise patterns for the site you are watching and keep the rest of the pipeline intact.

## When to use it

- Watching a regulator, registry or compliance page where a wording change matters and a rerender does not
- Diffing yesterday's scrape against today's in a cron job or GitHub Actions run, where a false alert costs credibility
- Comparing a Playwright render against a stored baseline in CI, to catch content regressions a snapshot test misses
- Monitoring third party API docs for removed endpoints or changed parameter tables
- Checking that a pricing page did not silently change after a CMS deploy
- Feeding a summarizer only the blocks that changed, instead of the whole page every run

## How it works

The public surface is two functions. `createStableHtmlSnapshot(html, options)` turns one HTML string into a `StableHtmlSnapshot`. `diffStableHtml(previousHtml, currentHtml, options)` snapshots both sides and returns a `StableHtmlDiffResult`. Everything else is private.

Snapshotting runs in four passes. `sanitizeHtml` strips comments and doctypes, then deletes the full body of every ignored tag. The default ignore set is `script`, `style`, `noscript`, `svg`, `canvas`, `iframe` and `template`, and anything you pass in `ignoreTagNames` is added on top rather than replacing it. It then lowercases every tag name and rewrites attributes through `normalizeAttributes`, which drops anything matching `DEFAULT_DYNAMIC_ATTRIBUTE_PATTERNS`: `id`, `nonce`, `integrity`, `crossorigin`, `aria-describedby` and the React, Vue, Next.js and hydration data attributes. Survivors are sorted alphabetically, so a framework reordering its attributes produces no delta.

Next, `extractSemanticBlocks` pulls typed blocks out with regex: the `<title>`, `h1` through `h6` as headings, `p` as paragraphs, `li` as list items, `tr` as table rows and `a` as links. Links are special cased so the block text carries the anchor text plus the href in parentheses, which is how a changed destination becomes visible. Every candidate goes through `normalizeExtractedText`, which strips inner tags, decodes entities, scrubs dynamic text and collapses whitespace. `scrubDynamicText` applies `DEFAULT_DYNAMIC_TEXT_PATTERNS`, covering updated at, last checked, request ID, session ID, trace ID, CSRF, nonce, build ID, commit SHAs, ISO 8601 timestamps and any hex run of 16 or more characters, then deletes RFC 4122 UUIDs and bare 10 to 13 digit numbers. Unless you set `preserveNumbers`, runs of 6 or more digits collapse to `#`. A block with no token at least `minTokenLength` characters long is dropped, as is anything matching `ignoreTextPatterns`, which by default kills cookie banners, skip links and the "enable javascript" placeholder. If nothing matches, the stripped body becomes one fallback paragraph, so an empty snapshot never silently compares equal.

Each surviving block gets a `key` and a `fingerprint`. The fingerprint is `sha256(kind|text)`. The key is `kind:` plus the first 16 hex characters of `sha256` over a seed and the lowercased first 180 characters of the text. The seed is the href for links, a fixed marker for the title and the stripped body text otherwise. `dedupeBlocks` collapses exact `kind|text` repeats, so a nav item in both header and footer counts once. The snapshot exposes three hashes: `blockFingerprint` over block fingerprints, `normalizedTextFingerprint` over the joined text and a composite `fingerprint` that also folds in the sanitized raw markup, so you pick how strict you want to be. The `changed` boolean ignores the strict composite and trips only when `normalizedTextFingerprint` or `blockFingerprint` moved.

The diff is a keyed set comparison over two `Map`s. Keys only in the previous snapshot are removals, keys only in the current one are additions and a shared key with a differing fingerprint is a modification. Counters are exact, but the `diff` array is capped by `pushCapped` at `maxDiffItems`, default 20, so a page that rewrote itself does not dump ten thousand entries into your alert payload.

## Usage

```bash
# CLI: two HTML files, optional cap on diff entries. Prints JSON to stdout.
npx tsx StableHtmlChangeDetector.ts baseline.html fresh.html
npx tsx StableHtmlChangeDetector.ts yesterday.html today.html 50
```

```ts
import {
  createStableHtmlSnapshot,
  diffStableHtml,
  type StableHtmlDetectorOptions,
} from './StableHtmlChangeDetector.js';

const options: StableHtmlDetectorOptions = {
  minTokenLength: 3,
  maxDiffItems: 40,
  preserveNumbers: true,
  ignoreTagNames: ['footer'],
  dynamicTextPatterns: [/\bvisitor count\b.*$/i],
  ignoreTextPatterns: [/^subscribe to our newsletter$/i],
};

// Store this alongside the scrape and compare fingerprints on the next run.
const baseline = createStableHtmlSnapshot(await fetchHtml(url), options);
console.log(baseline.blockFingerprint, baseline.blockCount, baseline.textLength);

const result = diffStableHtml(previousHtml, currentHtml, options);
if (result.changed) {
  console.log(`+${result.additions} -${result.removals} ~${result.modifications}`);
  for (const item of result.diff) {
    console.log(item.kind, item.key, item.before, '->', item.after);
  }
}
```

## Notes

- Regex based, not a DOM parser. Nested same name tags break the non greedy match, so a `<li>` containing another `<li>` or an unclosed `<script>` extracts oddly. Fine for server rendered and framework rendered pages, not for hand written soup.
- Block keys hash the lowercased first 180 characters of the text, so most edits change the key and surface as a removal plus an addition, not a `changed` item. A `changed` item appears only when two blocks share those 180 characters but differ later or differ in case. Read `additions` and `removals` together, not `modifications` alone.
- `ignoreTagNames` merges with the defaults, but `dynamicTextPatterns`, `dynamicAttributePatterns` and `ignoreTextPatterns` fully replace their defaults when passed. To extend those, spread the defaults into your own list on the calling side.
- `decodeHtmlEntities` covers seven entities only: `&nbsp;`, `&amp;`, `&lt;`, `&gt;`, `&quot;`, `&#39;` and `&#x2F;`. Anything else passes through literally and shows up in the block text.
- It fetches nothing. No HTTP client, no headless browser, no scheduler, no state store. You bring the HTML and persist the snapshot yourself.
- ESM only. Direct execution is detected by comparing `fileURLToPath(import.meta.url)` against `process.argv[1]`, so it needs `tsx`, `ts-node` in ESM mode or a compiled `.mjs`. A bad argument count or a non positive `maxDiffItems` sets exit code 1. A successful run exits 0 whether or not the page changed.
- Fingerprints are SHA-256 hex from `node:crypto`, not stable across versions of this file. Changing a pattern invalidates every stored baseline, so version your snapshots when you tune the rules.
