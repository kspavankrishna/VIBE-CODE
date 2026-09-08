import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';

export interface StableHtmlDetectorOptions {
  readonly minTokenLength?: number;
  readonly dynamicTextPatterns?: ReadonlyArray<RegExp>;
  readonly dynamicAttributePatterns?: ReadonlyArray<RegExp>;
  readonly ignoreTextPatterns?: ReadonlyArray<RegExp>;
  readonly ignoreTagNames?: ReadonlyArray<string>;
  readonly preserveNumbers?: boolean;
  readonly maxDiffItems?: number;
}

export interface SemanticBlock {
  readonly kind: 'title' | 'heading' | 'paragraph' | 'list-item' | 'table-row' | 'link';
  readonly key: string;
  readonly text: string;
  readonly fingerprint: string;
}

export interface StableHtmlSnapshot {
  readonly fingerprint: string;
  readonly normalizedTextFingerprint: string;
  readonly blockFingerprint: string;
  readonly blockCount: number;
  readonly textLength: number;
  readonly blocks: ReadonlyArray<SemanticBlock>;
}

export interface StableHtmlDiffItem {
  readonly kind: 'added' | 'removed' | 'changed';
  readonly key: string;
  readonly before?: string;
  readonly after?: string;
}

export interface StableHtmlDiffResult {
  readonly changed: boolean;
  readonly previous: StableHtmlSnapshot;
  readonly current: StableHtmlSnapshot;
  readonly additions: number;
  readonly removals: number;
  readonly modifications: number;
  readonly diff: ReadonlyArray<StableHtmlDiffItem>;
}

const DEFAULT_DYNAMIC_TEXT_PATTERNS: RegExp[] = [
  /\bupdated at\b.*$/i,
  /\blast checked\b.*$/i,
  /\blast refreshed\b.*$/i,
  /\bcache bust\b.*$/i,
  /\brequest id\b.*$/i,
  /\bsession id\b.*$/i,
  /\btrace id\b.*$/i,
  /\bcsrf\b.*$/i,
  /\bnonce\b.*$/i,
  /\bbuild id\b.*$/i,
  /\bcommit\b\s+[a-f0-9]{7,40}\b/i,
  /\bversion\b\s+\d{4}[.-]\d{1,2}[.-]\d{1,2}\b/i,
  /\b\d{4}-\d{2}-\d{2}[t\s]\d{2}:\d{2}(:\d{2})?(\.\d+)?(z|[+-]\d{2}:?\d{2})?\b/i,
  /\b[a-f0-9]{16,}\b/i,
];

const DEFAULT_DYNAMIC_ATTRIBUTE_PATTERNS: RegExp[] = [
  /^data-react(root|id)?$/i,
  /^data-v-/i,
  /^data-nextjs/i,
  /^data-hydration/i,
  /^data-rh/i,
  /^nonce$/i,
  /^integrity$/i,
  /^crossorigin$/i,
  /^aria-describedby$/i,
  /^id$/i,
];

const DEFAULT_IGNORE_TEXT_PATTERNS: RegExp[] = [
  /^\s*$/, 
  /^enable javascript to run this app\.?$/i,
  /^cookie preferences?$/i,
  /^accept all cookies?$/i,
  /^skip to main content$/i,
];

const DEFAULT_IGNORE_TAGS = new Set([
  'script',
  'style',
  'noscript',
  'svg',
  'canvas',
  'iframe',
  'template',
]);

export function createStableHtmlSnapshot(
  html: string,
  options: StableHtmlDetectorOptions = {},
): StableHtmlSnapshot {
  const blocks = extractSemanticBlocks(html, options);
  const normalizedText = blocks.map((block) => block.text).join('\n');

  return {
    fingerprint: hash([
      hash(normalizeWhitespace(stripHtmlForRawFingerprint(html, options))),
      hash(normalizedText),
      hash(blocks.map((block) => `${block.kind}|${block.key}|${block.fingerprint}`).join('\n')),
    ].join('|')),
    normalizedTextFingerprint: hash(normalizedText),
    blockFingerprint: hash(blocks.map((block) => block.fingerprint).join('\n')),
    blockCount: blocks.length,
    textLength: normalizedText.length,
    blocks,
  };
}

export function diffStableHtml(
  previousHtml: string,
  currentHtml: string,
  options: StableHtmlDetectorOptions = {},
): StableHtmlDiffResult {
  const previous = createStableHtmlSnapshot(previousHtml, options);
  const current = createStableHtmlSnapshot(currentHtml, options);
  const maxDiffItems = options.maxDiffItems ?? 20;

  const previousMap = new Map(previous.blocks.map((block) => [block.key, block]));
  const currentMap = new Map(current.blocks.map((block) => [block.key, block]));
  const diff: StableHtmlDiffItem[] = [];
  let additions = 0;
  let removals = 0;
  let modifications = 0;

  for (const [key, before] of previousMap) {
    const after = currentMap.get(key);
    if (!after) {
      removals += 1;
      pushCapped(diff, maxDiffItems, { kind: 'removed', key, before: before.text });
      continue;
    }

    if (before.fingerprint !== after.fingerprint) {
      modifications += 1;
      pushCapped(diff, maxDiffItems, {
        kind: 'changed',
        key,
        before: before.text,
        after: after.text,
      });
    }
  }

  for (const [key, after] of currentMap) {
    if (previousMap.has(key)) {
      continue;
    }

    additions += 1;
    pushCapped(diff, maxDiffItems, { kind: 'added', key, after: after.text });
  }

  return {
    changed:
      previous.normalizedTextFingerprint !== current.normalizedTextFingerprint ||
      previous.blockFingerprint !== current.blockFingerprint,
    previous,
    current,
    additions,
    removals,
    modifications,
    diff,
  };
}

function extractSemanticBlocks(
  html: string,
  options: StableHtmlDetectorOptions,
): SemanticBlock[] {
  const sanitized = sanitizeHtml(html, options);
  const blocks: SemanticBlock[] = [];
  const pushBlock = (kind: SemanticBlock['kind'], keySeed: string, text: string) => {
    const normalized = normalizeExtractedText(text, options);
    if (!normalized) {
      return;
    }

    if (shouldIgnoreText(normalized, options)) {
      return;
    }

    const key = `${kind}:${stableKey(keySeed, normalized)}`;
    blocks.push({
      kind,
      key,
      text: normalized,
      fingerprint: hash(`${kind}|${normalized}`),
    });
  };

  const titleMatch = sanitized.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
  if (titleMatch) {
    pushBlock('title', 'document-title', decodeHtmlEntities(titleMatch[1]));
  }

  const blockPatterns: Array<{ kind: SemanticBlock['kind']; regex: RegExp }> = [
    { kind: 'heading', regex: /<(h[1-6])\b[^>]*>([\s\S]*?)<\/\1>/gi },
    { kind: 'paragraph', regex: /<p\b[^>]*>([\s\S]*?)<\/p>/gi },
    { kind: 'list-item', regex: /<li\b[^>]*>([\s\S]*?)<\/li>/gi },
    { kind: 'table-row', regex: /<tr\b[^>]*>([\s\S]*?)<\/tr>/gi },
    { kind: 'link', regex: /<a\b([^>]*)>([\s\S]*?)<\/a>/gi },
  ];

  for (const { kind, regex } of blockPatterns) {
    for (const match of sanitized.matchAll(regex)) {
      if (kind === 'link') {
        const attrs = match[1] ?? '';
        const href = extractAttribute(attrs, 'href');
        const body = match[2] ?? '';
        pushBlock(kind, href || stripTags(body), `${stripTags(body)} ${href ? `(${href})` : ''}`);
        continue;
      }

      const body = match[2] ?? match[1] ?? '';
      pushBlock(kind, stripTags(body), body);
    }
  }

  if (blocks.length === 0) {
    const fallback = normalizeExtractedText(stripTags(sanitized), options);
    if (fallback) {
      pushBlock('paragraph', 'fallback-body', fallback);
    }
  }

  return dedupeBlocks(blocks);
}

function sanitizeHtml(html: string, options: StableHtmlDetectorOptions): string {
  const ignoreTagNames = new Set(
    (options.ignoreTagNames ?? []).map((tag) => tag.toLowerCase()),
  );
  for (const tag of DEFAULT_IGNORE_TAGS) {
    ignoreTagNames.add(tag);
  }

  let sanitized = html
    .replace(/<!--([\s\S]*?)-->/g, ' ')
    .replace(/<!(doctype|DOCTYPE)[^>]*>/g, ' ');

  for (const tag of ignoreTagNames) {
    sanitized = sanitized.replace(
      new RegExp(`<${tag}\\b[^>]*>[\\s\\S]*?<\\/${tag}>`, 'gi'),
      ' ',
    );
  }

  sanitized = sanitized.replace(/<([a-z0-9:-]+)([^>]*)>/gi, (_full, tagName: string, attrs: string) => {
    const normalizedAttrs = normalizeAttributes(attrs, options);
    return `<${tagName.toLowerCase()}${normalizedAttrs}>`;
  });

  return sanitized;
}

function stripHtmlForRawFingerprint(html: string, options: StableHtmlDetectorOptions): string {
  return sanitizeHtml(html, options)
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeAttributes(attrs: string, options: StableHtmlDetectorOptions): string {
  const dynamicPatterns = options.dynamicAttributePatterns ?? DEFAULT_DYNAMIC_ATTRIBUTE_PATTERNS;
  const kept: string[] = [];
  const attrRegex = /([:\w-]+)(?:\s*=\s*("([^"]*)"|'([^']*)'|([^\s"'>]+)))?/g;

  for (const match of attrs.matchAll(attrRegex)) {
    const name = (match[1] ?? '').toLowerCase();
    const rawValue = match[3] ?? match[4] ?? match[5] ?? '';
    if (!name || dynamicPatterns.some((pattern) => pattern.test(name))) {
      continue;
    }

    let value = normalizeWhitespace(decodeHtmlEntities(rawValue));
    if (!value) {
      kept.push(name);
      continue;
    }

    value = scrubDynamicText(value, options);
    if (!value) {
      continue;
    }

    kept.push(`${name}="${value}"`);
  }

  kept.sort();
  return kept.length > 0 ? ` ${kept.join(' ')}` : '';
}

function normalizeExtractedText(text: string, options: StableHtmlDetectorOptions): string {
  let normalized = decodeHtmlEntities(stripTags(text));
  normalized = scrubDynamicText(normalized, options);
  normalized = normalizeWhitespace(normalized);

  if (!options.preserveNumbers) {
    normalized = normalized.replace(/\b\d{6,}\b/g, '#');
  }

  const minTokenLength = options.minTokenLength ?? 2;
  const tokenCount = normalized.split(/\s+/).filter((token) => token.length >= minTokenLength).length;
  return tokenCount === 0 ? '' : normalized;
}

function scrubDynamicText(text: string, options: StableHtmlDetectorOptions): string {
  const dynamicPatterns = options.dynamicTextPatterns ?? DEFAULT_DYNAMIC_TEXT_PATTERNS;
  let scrubbed = text;
  for (const pattern of dynamicPatterns) {
    scrubbed = scrubbed.replace(pattern, ' ');
  }

  return scrubbed
    .replace(/\b[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\b/gi, ' ')
    .replace(/\b\d{10,13}\b/g, ' ');
}

function shouldIgnoreText(text: string, options: StableHtmlDetectorOptions): boolean {
  const ignorePatterns = options.ignoreTextPatterns ?? DEFAULT_IGNORE_TEXT_PATTERNS;
  return ignorePatterns.some((pattern) => pattern.test(text));
}

function extractAttribute(attrs: string, attributeName: string): string | undefined {
  const regex = new RegExp(`${attributeName}\\s*=\\s*("([^"]*)"|'([^']*)'|([^\\s"'>]+))`, 'i');
  const match = attrs.match(regex);
  return decodeHtmlEntities(match?.[2] ?? match?.[3] ?? match?.[4] ?? '').trim() || undefined;
}

function stripTags(value: string): string {
  return value.replace(/<[^>]+>/g, ' ');
}

function decodeHtmlEntities(value: string): string {
  return value
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/&#x2F;/gi, '/');
}

function normalizeWhitespace(value: string): string {
  return value.replace(/\s+/g, ' ').trim();
}

function stableKey(seed: string, text: string): string {
  const normalizedSeed = normalizeWhitespace(seed).toLowerCase().slice(0, 80);
  const normalizedText = text.toLowerCase().slice(0, 180);
  return hash(`${normalizedSeed}|${normalizedText}`).slice(0, 16);
}

function dedupeBlocks(blocks: ReadonlyArray<SemanticBlock>): SemanticBlock[] {
  const seen = new Set<string>();
  const deduped: SemanticBlock[] = [];
  for (const block of blocks) {
    const identity = `${block.kind}|${block.text}`;
    if (seen.has(identity)) {
      continue;
    }

    seen.add(identity);
    deduped.push(block);
  }

  return deduped;
}

function pushCapped<T>(items: T[], maxItems: number, value: T): void {
  if (items.length < maxItems) {
    items.push(value);
  }
}

function hash(value: string): string {
  return createHash('sha256').update(value).digest('hex');
}

async function runCli(): Promise<void> {
  const args = process.argv.slice(2);
  if (args.length < 2 || args.length > 3) {
    console.error(
      'Usage: tsx StableHtmlChangeDetector.ts <previous.html> <current.html> [maxDiffItems]',
    );
    process.exitCode = 1;
    return;
  }

  const [previousPath, currentPath, maxDiffItemsArg] = args;
  const maxDiffItems = maxDiffItemsArg ? Number.parseInt(maxDiffItemsArg, 10) : 20;
  if (!Number.isFinite(maxDiffItems) || maxDiffItems <= 0) {
    throw new Error(`Invalid maxDiffItems value: ${maxDiffItemsArg}`);
  }

  const [previousHtml, currentHtml] = await Promise.all([
    readFile(previousPath, 'utf8'),
    readFile(currentPath, 'utf8'),
  ]);

  const result = diffStableHtml(previousHtml, currentHtml, { maxDiffItems });
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
}

const isExecutedDirectly = (() => {
  try {
    return fileURLToPath(import.meta.url) === process.argv[1];
  } catch {
    return false;
  }
})();

if (isExecutedDirectly) {
  runCli().catch((error: unknown) => {
    const message = error instanceof Error ? error.stack ?? error.message : String(error);
    console.error(message);
    process.exitCode = 1;
  });
}

/*
This solves flaky website change detection for teams that monitor public HTML pages and only want alerts when the page meaning actually changed. Built because a lot of April 2026 monitoring work still breaks on modern frontend churn: hydration IDs, nonce values, session tokens, timestamp banners, rotating hashes, and tiny markup shifts make raw checksums noisy and useless. Use it when you need to watch docs pages, registry pages, compliance pages, research portals, dashboards rendered to HTML, or any browser-captured output where the structure stays mostly the same but the important text can change.

The trick: instead of trusting the full raw HTML, this file strips unstable tags, normalizes attributes, scrubs dynamic text patterns, extracts semantic blocks like titles, headings, paragraphs, list items, table rows, and links, and then fingerprints those stable blocks. That gives you a much better signal for “did the content people care about change” versus “did the frontend framework reshuffle internals again.” The diff output is intentionally practical: added, removed, and changed blocks with stable keys, so you can plug it into CI, cron monitors, Playwright scrapers, GitHub Actions, alerting pipelines, or edge jobs without having to build your own classifier first.

Drop this into a Node or TypeScript repo that captures HTML snapshots, then compare yesterday vs today, baseline vs fresh scrape, or expected vs actual render. I wrote it in plain TypeScript with only Node built-ins so it is easy to fork, audit, and adapt. If your target site has its own noise patterns, add them to the options instead of rewriting the core. That is the main reason this is useful in production: you keep the stable pipeline, and only tune the ignore rules for each monitored site.
*/