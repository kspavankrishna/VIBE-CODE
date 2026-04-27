<?php
declare(strict_types=1);

final class HtmlPromptInjectionFirewallConfig
{
    public function __construct(
        public int $maxOutputChars = 120000,
        public int $maxCommentChars = 8000,
        public int $maxTextPerNode = 6000,
        public int $maxLinks = 200,
        public int $highEntropyMinLength = 28,
        public bool $includeImageAltText = true,
        public bool $includeLinks = true,
        public bool $preserveTables = true,
        public bool $dropHiddenNodes = true,
        public bool $dropVisiblePromptLikeText = false,
        public bool $dedupeBlocks = true,
        public float $reviewThreshold = 0.55,
        public array $allowedLinkSchemes = ['http', 'https', 'mailto'],
        public array $blockedTags = [
            'script', 'style', 'noscript', 'template', 'iframe', 'object', 'embed',
            'svg', 'canvas', 'meta', 'link', 'base', 'form', 'input', 'textarea',
            'select', 'option', 'button', 'label'
        ]
    ) {
    }
}

final class HtmlPromptInjectionFinding implements JsonSerializable
{
    public function __construct(
        public string $kind,
        public string $severity,
        public string $message,
        public string $snippet,
        public array $meta = []
    ) {
    }

    public function jsonSerialize(): array
    {
        return [
            'kind' => $this->kind,
            'severity' => $this->severity,
            'message' => $this->message,
            'snippet' => $this->snippet,
            'meta' => $this->meta,
        ];
    }
}

final class HtmlPromptInjectionResult implements JsonSerializable
{
    /** @param HtmlPromptInjectionFinding[] $findings */
    public function __construct(
        public string $text,
        public array $findings,
        public float $riskScore,
        public bool $needsReview,
        public array $stats,
        public array $quarantine,
        public ?string $sourceUrl = null
    ) {
    }

    public function jsonSerialize(): array
    {
        return [
            'source_url' => $this->sourceUrl,
            'text' => $this->text,
            'risk_score' => $this->riskScore,
            'needs_review' => $this->needsReview,
            'stats' => $this->stats,
            'quarantine' => $this->quarantine,
            'findings' => array_map(
                static fn (HtmlPromptInjectionFinding $finding): array => $finding->jsonSerialize(),
                $this->findings
            ),
        ];
    }
}

final class HtmlPromptInjectionTextAccumulator
{
    private array $blocks = [];
    private string $inline = '';
    private int $length = 0;
    public bool $truncated = false;

    public function __construct(
        private readonly int $maxChars,
        private readonly bool $dedupeBlocks
    ) {
    }

    public function appendInline(string $text): void
    {
        if ($this->truncated || $text === '') {
            return;
        }

        $text = $this->normalizeInline($text);
        if ($text === '') {
            return;
        }

        if (
            $this->inline !== ''
            && !preg_match('/[\s(\[{\/-]$/u', $this->inline)
            && !preg_match('/^[,.;:!?%\])}]/u', $text)
        ) {
            $this->inline .= ' ';
        }

        $this->inline .= $text;
    }

    public function lineBreak(): void
    {
        if ($this->truncated) {
            return;
        }

        $this->inline = rtrim($this->inline) . "\n";
    }

    public function flushBlock(): void
    {
        if ($this->truncated) {
            return;
        }

        $block = $this->normalizeBlock($this->inline);
        $this->inline = '';
        if ($block !== '') {
            $this->addBlock($block);
        }
    }

    public function pushBlock(string $block): void
    {
        if ($this->truncated) {
            return;
        }

        $this->flushBlock();
        $block = $this->normalizeBlock($block);
        if ($block !== '') {
            $this->addBlock($block);
        }
    }

    public function render(): string
    {
        $this->flushBlock();
        return implode("\n\n", $this->blocks);
    }

    private function addBlock(string $block): void
    {
        if ($this->dedupeBlocks && !empty($this->blocks) && end($this->blocks) === $block) {
            return;
        }

        $separator = empty($this->blocks) ? 0 : 2;
        $remaining = $this->maxChars - $this->length - $separator;

        if ($remaining <= 0) {
            $this->truncated = true;
            return;
        }

        if ($this->stringLength($block) > $remaining) {
            $block = $this->stringSlice($block, max(0, $remaining - 22)) . "\n\n[TRUNCATED OUTPUT]";
            $this->truncated = true;
        }

        $this->blocks[] = $block;
        $this->length += $separator + $this->stringLength($block);
    }

    private function normalizeInline(string $text): string
    {
        $text = preg_replace('/[ \t\x0B\f\r]+/u', ' ', $text) ?? $text;
        $text = preg_replace('/ ?\n ?/u', "\n", $text) ?? $text;
        return trim($text);
    }

    private function normalizeBlock(string $text): string
    {
        $text = preg_replace('/[ \t]+/u', ' ', $text) ?? $text;
        $text = preg_replace('/\n{3,}/u', "\n\n", $text) ?? $text;
        $text = preg_replace('/ +\n/u', "\n", $text) ?? $text;
        $text = preg_replace('/\n +/u', "\n", $text) ?? $text;
        return trim($text);
    }

    private function stringLength(string $value): int
    {
        return function_exists('mb_strlen') ? mb_strlen($value, 'UTF-8') : strlen($value);
    }

    private function stringSlice(string $value, int $length): string
    {
        if ($length <= 0) {
            return '';
        }

        return function_exists('mb_substr')
            ? mb_substr($value, 0, $length, 'UTF-8')
            : substr($value, 0, $length);
    }
}

final class HtmlPromptInjectionFirewall
{
    private const CONTROL_PATTERN = '/[\x{200B}-\x{200F}\x{202A}-\x{202E}\x{2066}-\x{2069}\x{FEFF}]/u';
    private const COMMENT_PATTERN = '/<!--(.*?)-->/su';
    private const HIGH_ENTROPY_TOKEN_PATTERN = '/[A-Za-z0-9+\/=_.:-]{28,}/';
    private const BLOCK_TAGS = [
        'article' => true,
        'aside' => true,
        'blockquote' => true,
        'div' => true,
        'footer' => true,
        'header' => true,
        'main' => true,
        'nav' => true,
        'p' => true,
        'section' => true,
    ];
    private const PROMPT_DROP_TAGS = [
        'aside' => true,
        'div' => true,
        'footer' => true,
        'nav' => true,
        'p' => true,
        'section' => true,
        'small' => true,
        'span' => true,
    ];
    private const HEADING_LEVELS = [
        'h1' => 1,
        'h2' => 2,
        'h3' => 3,
        'h4' => 4,
        'h5' => 5,
        'h6' => 6,
    ];
    private const PROMPT_RULES = [
        [
            'label' => 'ignore-prior-instructions',
            'regex' => '/\b(ignore|disregard|forget)\b.{0,40}\b(previous|prior|above|system|developer)\b.{0,20}\b(instructions|prompt|message|rules)\b/iu',
            'severity' => 'critical',
            'message' => 'Text tries to override earlier instructions.',
        ],
        [
            'label' => 'prompt-exfiltration',
            'regex' => '/\b(reveal|show|dump|print|extract)\b.{0,40}\b(system|developer|hidden)\b.{0,20}\b(prompt|message|instructions)\b/iu',
            'severity' => 'critical',
            'message' => 'Text asks for system or developer prompt material.',
        ],
        [
            'label' => 'silent-exfiltration',
            'regex' => '/\b(do not|don\'t)\b.{0,30}\b(tell|inform|mention)\b.{0,30}\buser\b/iu',
            'severity' => 'high',
            'message' => 'Text explicitly asks the model to hide behavior from the user.',
        ],
        [
            'label' => 'tool-steering',
            'regex' => '/\b(tool call|tool result|function call|browser action|click this link|navigate to)\b/iu',
            'severity' => 'high',
            'message' => 'Text contains direct tool or browser steering language.',
        ],
        [
            'label' => 'credential-exfiltration',
            'regex' => '/\b(exfiltrate|send|post|leak|steal)\b.{0,40}\b(token|cookie|secret|credential|header)\b/iu',
            'severity' => 'critical',
            'message' => 'Text asks for secrets, credentials, or session material.',
        ],
    ];

    private readonly HtmlPromptInjectionFirewallConfig $config;

    public function __construct(?HtmlPromptInjectionFirewallConfig $config = null)
    {
        $this->config = $config ?? new HtmlPromptInjectionFirewallConfig();
    }

    public function sanitize(string $html, ?string $sourceUrl = null): HtmlPromptInjectionResult
    {
        $stats = [
            'input_bytes' => strlen($html),
            'output_chars' => 0,
            'comment_count' => 0,
            'dropped_nodes' => 0,
            'dropped_hidden_nodes' => 0,
            'dropped_blocked_tags' => 0,
            'links_retained' => 0,
            'truncated' => false,
        ];
        $findings = [];
        $quarantine = [];

        if (trim($html) === '') {
            return new HtmlPromptInjectionResult('', [], 0.0, false, $stats, [], $sourceUrl);
        }

        $this->scanRawHtml($html, $findings, $quarantine, $stats);
        $dom = $this->loadDocument($html);

        if ($dom === null) {
            $text = $this->normalizeText(strip_tags($html));
            $stats['output_chars'] = $this->stringLength($text);
            foreach ($this->findHighEntropyFindings($text, 'fallback-text') as $finding) {
                $findings[] = $finding;
            }
            $risk = $this->computeRiskScore($findings);
            return new HtmlPromptInjectionResult(
                $text,
                $findings,
                $risk,
                $risk >= $this->config->reviewThreshold || $this->hasCriticalFinding($findings),
                $stats,
                $quarantine,
                $sourceUrl
            );
        }

        $this->pruneDocument($dom, $findings, $quarantine, $stats);
        $root = $dom->getElementsByTagName('body')->item(0) ?? $dom->documentElement;
        $accumulator = new HtmlPromptInjectionTextAccumulator($this->config->maxOutputChars, $this->config->dedupeBlocks);

        if ($root !== null) {
            $this->walkNode($root, $accumulator, $findings, $stats, 0);
        }

        $text = $accumulator->render();
        if ($text === '') {
            $text = $this->normalizeText(strip_tags($html));
        }

        foreach ($this->findHighEntropyFindings($text, 'extracted-text') as $finding) {
            $findings[] = $finding;
        }

        $stats['truncated'] = $accumulator->truncated;
        $stats['output_chars'] = $this->stringLength($text);
        $risk = $this->computeRiskScore($findings);

        return new HtmlPromptInjectionResult(
            $text,
            $findings,
            $risk,
            $risk >= $this->config->reviewThreshold || $this->hasCriticalFinding($findings),
            $stats,
            array_values(array_unique($quarantine)),
            $sourceUrl
        );
    }

    private function scanRawHtml(string $html, array &$findings, array &$quarantine, array &$stats): void
    {
        if (preg_match_all(self::COMMENT_PATTERN, $html, $matches, PREG_SET_ORDER)) {
            foreach ($matches as $match) {
                $comment = $this->normalizeText(html_entity_decode($match[1], ENT_QUOTES | ENT_HTML5, 'UTF-8'));
                if ($comment === '') {
                    continue;
                }

                $stats['comment_count']++;
                $snippet = $this->summarize($comment, $this->config->maxCommentChars);
                $signals = $this->matchPromptRules($comment);
                foreach ($signals as $signal) {
                    $findings[] = new HtmlPromptInjectionFinding(
                        'html-comment',
                        $signal['severity'],
                        $signal['message'],
                        $snippet,
                        ['rule' => $signal['label']]
                    );
                    $quarantine[] = $snippet;
                }

                if ($this->containsControlCharacters($comment)) {
                    $findings[] = new HtmlPromptInjectionFinding(
                        'comment-control-characters',
                        'medium',
                        'Comment contains zero-width or bidi control characters.',
                        $snippet
                    );
                    $quarantine[] = $snippet;
                }
            }
        }

        if ($this->containsControlCharacters($html)) {
            $findings[] = new HtmlPromptInjectionFinding(
                'unicode-control-characters',
                'medium',
                'Raw HTML contains zero-width or bidi control characters.',
                $this->summarize($html, 240)
            );
        }

        if (preg_match('/\b(?:javascript|data):/iu', $html)) {
            $findings[] = new HtmlPromptInjectionFinding(
                'dangerous-link-scheme',
                'high',
                'Raw HTML references javascript: or data: URLs.',
                $this->summarize($html, 240)
            );
        }

        if (preg_match('/<meta\b[^>]*http-equiv\s*=\s*["\']?refresh/iu', $html)) {
            $findings[] = new HtmlPromptInjectionFinding(
                'meta-refresh',
                'high',
                'Raw HTML contains a meta refresh directive.',
                $this->summarize($html, 240)
            );
        }
    }

    private function loadDocument(string $html): ?DOMDocument
    {
        if (!class_exists(DOMDocument::class)) {
            return null;
        }

        $dom = new DOMDocument('1.0', 'UTF-8');
        $previous = libxml_use_internal_errors(true);
        $flags = LIBXML_NOWARNING | LIBXML_NOERROR | LIBXML_COMPACT;
        if (defined('LIBXML_HTML_NOIMPLIED')) {
            $flags |= LIBXML_HTML_NOIMPLIED;
        }
        if (defined('LIBXML_HTML_NODEFDTD')) {
            $flags |= LIBXML_HTML_NODEFDTD;
        }

        $loaded = $dom->loadHTML('<?xml encoding="utf-8" ?>' . $html, $flags);
        if ($dom->firstChild !== null && $dom->firstChild->nodeType === XML_PI_NODE) {
            $dom->removeChild($dom->firstChild);
        }

        libxml_clear_errors();
        libxml_use_internal_errors($previous);

        return $loaded ? $dom : null;
    }

    private function pruneDocument(DOMDocument $dom, array &$findings, array &$quarantine, array &$stats): void
    {
        $xpath = new DOMXPath($dom);
        $nodeList = $xpath->query('//* | //comment()');
        if ($nodeList === false) {
            return;
        }

        $toRemove = [];
        foreach ($this->nodeListToArray($nodeList) as $node) {
            if ($node instanceof DOMComment) {
                $toRemove[] = $node;
                continue;
            }

            if (!$node instanceof DOMElement) {
                continue;
            }

            $tag = strtolower($node->tagName);
            if (in_array($tag, $this->config->blockedTags, true)) {
                $stats['dropped_nodes']++;
                $stats['dropped_blocked_tags']++;
                $snippet = $this->summarize($this->normalizeText($node->textContent ?? ''), 240);
                if ($snippet !== '') {
                    $quarantine[] = $snippet;
                }
                $findings[] = new HtmlPromptInjectionFinding(
                    'blocked-tag',
                    'medium',
                    sprintf('Dropped <%s> before LLM extraction.', $tag),
                    $snippet !== '' ? $snippet : sprintf('<%s>', $tag),
                    ['tag' => $tag]
                );
                $toRemove[] = $node;
                continue;
            }

            if ($this->config->dropHiddenNodes && $this->isHiddenElement($node)) {
                $stats['dropped_nodes']++;
                $stats['dropped_hidden_nodes']++;
                $text = $this->normalizeText($node->textContent ?? '');
                $signals = $this->matchPromptRules($text);
                $severity = !empty($signals) ? 'critical' : 'high';
                $snippet = $this->summarize($text !== '' ? $text : $node->getAttribute('style'), 240);
                $findings[] = new HtmlPromptInjectionFinding(
                    'hidden-node',
                    $severity,
                    'Dropped hidden or off-screen DOM content.',
                    $snippet,
                    ['tag' => $tag]
                );
                foreach ($signals as $signal) {
                    $findings[] = new HtmlPromptInjectionFinding(
                        'hidden-prompt-signal',
                        $signal['severity'],
                        $signal['message'],
                        $snippet,
                        ['tag' => $tag, 'rule' => $signal['label']]
                    );
                }
                if ($snippet !== '') {
                    $quarantine[] = $snippet;
                }
                $toRemove[] = $node;
                continue;
            }

            $this->inspectElementAttributes($node, $findings);

            if (
                $this->config->dropVisiblePromptLikeText
                && isset(self::PROMPT_DROP_TAGS[$tag])
                && !$this->isCodeLikeContext($node)
            ) {
                $text = $this->normalizeText($node->textContent ?? '');
                $signals = $this->matchPromptRules($text);
                if ($text !== '' && !empty($signals)) {
                    $stats['dropped_nodes']++;
                    $snippet = $this->summarize($text, 240);
                    $findings[] = new HtmlPromptInjectionFinding(
                        'visible-prompt-signal',
                        'high',
                        'Dropped visible text because strict mode found prompt-like instructions.',
                        $snippet,
                        ['tag' => $tag]
                    );
                    foreach ($signals as $signal) {
                        $findings[] = new HtmlPromptInjectionFinding(
                            'visible-prompt-signal-rule',
                            $signal['severity'],
                            $signal['message'],
                            $snippet,
                            ['tag' => $tag, 'rule' => $signal['label']]
                        );
                    }
                    $quarantine[] = $snippet;
                    $toRemove[] = $node;
                }
            }
        }

        foreach ($toRemove as $node) {
            if ($node->parentNode !== null) {
                $node->parentNode->removeChild($node);
            }
        }
    }

    private function inspectElementAttributes(DOMElement $node, array &$findings): void
    {
        $tag = strtolower($node->tagName);
        foreach (['href', 'src', 'action', 'formaction'] as $attribute) {
            if (!$node->hasAttribute($attribute)) {
                continue;
            }

            $value = trim($node->getAttribute($attribute));
            if ($value === '') {
                continue;
            }

            if (!$this->isAllowedLinkScheme($value)) {
                $findings[] = new HtmlPromptInjectionFinding(
                    'dangerous-attribute-url',
                    'high',
                    sprintf('Found disallowed URL scheme in %s on <%s>.', $attribute, $tag),
                    $this->summarize($value, 180),
                    ['tag' => $tag, 'attribute' => $attribute]
                );
            }
        }

        if ($node->hasAttribute('style') && $this->styleImpliesHidden($node->getAttribute('style'))) {
            $findings[] = new HtmlPromptInjectionFinding(
                'hidden-style',
                'medium',
                sprintf('<%s> uses a style that hides content from humans.', $tag),
                $this->summarize($node->getAttribute('style'), 180),
                ['tag' => $tag]
            );
        }
    }

    private function walkNode(
        DOMNode $node,
        HtmlPromptInjectionTextAccumulator $accumulator,
        array &$findings,
        array &$stats,
        int $listDepth
    ): void {
        if ($node instanceof DOMText) {
            $text = $this->normalizeText($node->wholeText ?? '');
            if ($text !== '') {
                $accumulator->appendInline($text);
            }
            return;
        }

        if (!$node instanceof DOMElement) {
            return;
        }

        $tag = strtolower($node->tagName);

        if (isset(self::HEADING_LEVELS[$tag])) {
            $heading = $this->truncateText($this->extractInlineText($node, $findings, $stats, true), $this->config->maxTextPerNode);
            if ($heading !== '') {
                $accumulator->pushBlock(str_repeat('#', self::HEADING_LEVELS[$tag]) . ' ' . $heading);
            }
            return;
        }

        if ($tag === 'br') {
            $accumulator->lineBreak();
            return;
        }

        if ($tag === 'pre') {
            $text = trim($node->textContent ?? '');
            if ($text !== '') {
                $accumulator->pushBlock("```text\n" . $this->truncateText($text, $this->config->maxTextPerNode) . "\n```");
            }
            return;
        }

        if ($tag === 'table') {
            if ($this->config->preserveTables) {
                $table = $this->renderTable($node, $findings, $stats);
                if ($table !== '') {
                    $accumulator->pushBlock($table);
                }
            } else {
                $text = $this->truncateText($this->normalizeText($node->textContent ?? ''), $this->config->maxTextPerNode);
                if ($text !== '') {
                    $accumulator->pushBlock($text);
                }
            }
            return;
        }

        if ($tag === 'ul') {
            $this->renderList($node, $accumulator, $findings, $stats, false, $listDepth);
            return;
        }

        if ($tag === 'ol') {
            $this->renderList($node, $accumulator, $findings, $stats, true, $listDepth);
            return;
        }

        if ($tag === 'img') {
            if ($this->config->includeImageAltText) {
                $alt = trim($node->getAttribute('alt'));
                if ($alt !== '') {
                    $accumulator->appendInline('[Image: ' . $this->truncateText($alt, 240) . ']');
                }
            }
            return;
        }

        if ($tag === 'a') {
            $inline = $this->extractInlineText($node, $findings, $stats, true);
            if ($inline !== '') {
                $accumulator->appendInline($inline);
            }
            return;
        }

        $blockLevel = isset(self::BLOCK_TAGS[$tag]);
        if ($blockLevel) {
            $accumulator->flushBlock();
        }

        foreach ($this->nodeListToArray($node->childNodes) as $child) {
            $this->walkNode($child, $accumulator, $findings, $stats, $listDepth);
        }

        if ($blockLevel) {
            $accumulator->flushBlock();
        }
    }

    private function renderList(
        DOMElement $node,
        HtmlPromptInjectionTextAccumulator $accumulator,
        array &$findings,
        array &$stats,
        bool $ordered,
        int $depth
    ): void {
        $accumulator->flushBlock();
        $index = 1;
        foreach ($this->nodeListToArray($node->childNodes) as $child) {
            if (!$child instanceof DOMElement || strtolower($child->tagName) !== 'li') {
                continue;
            }

            $prefix = $ordered ? $index . '. ' : '- ';
            $indent = str_repeat('  ', max(0, $depth));
            $text = $this->truncateText($this->extractInlineText($child, $findings, $stats, true), $this->config->maxTextPerNode);
            if ($text !== '') {
                $accumulator->pushBlock($indent . $prefix . $text);
            }

            foreach ($this->nodeListToArray($child->childNodes) as $grandChild) {
                if (!$grandChild instanceof DOMElement) {
                    continue;
                }
                $tag = strtolower($grandChild->tagName);
                if ($tag === 'ul') {
                    $this->renderList($grandChild, $accumulator, $findings, $stats, false, $depth + 1);
                } elseif ($tag === 'ol') {
                    $this->renderList($grandChild, $accumulator, $findings, $stats, true, $depth + 1);
                }
            }

            $index++;
        }
    }

    private function renderTable(DOMElement $table, array &$findings, array &$stats): string
    {
        $rows = [];
        $hasHeader = false;

        foreach ($table->getElementsByTagName('tr') as $row) {
            $cells = [];
            $headerRow = false;
            foreach ($this->nodeListToArray($row->childNodes) as $cell) {
                if (!$cell instanceof DOMElement) {
                    continue;
                }
                $tag = strtolower($cell->tagName);
                if ($tag !== 'th' && $tag !== 'td') {
                    continue;
                }
                $headerRow = $headerRow || $tag === 'th';
                $cells[] = $this->truncateText($this->extractInlineText($cell, $findings, $stats, true), 400);
            }

            if (!empty($cells)) {
                $rows[] = '| ' . implode(' | ', $cells) . ' |';
                $hasHeader = $hasHeader || $headerRow;
            }
        }

        if (empty($rows)) {
            return '';
        }

        if ($hasHeader && count($rows) >= 1) {
            $columnCount = substr_count($rows[0], '|') - 1;
            $separator = '| ' . implode(' | ', array_fill(0, max(1, $columnCount), '---')) . ' |';
            array_splice($rows, 1, 0, [$separator]);
        }

        return implode("\n", $rows);
    }

    private function extractInlineText(DOMNode $node, array &$findings, array &$stats, bool $skipNestedLists): string
    {
        if ($node instanceof DOMText) {
            return $this->normalizeText($node->wholeText ?? '');
        }

        if (!$node instanceof DOMElement) {
            return '';
        }

        if ($this->config->dropHiddenNodes && $this->isHiddenElement($node)) {
            return '';
        }

        $tag = strtolower($node->tagName);
        if (in_array($tag, $this->config->blockedTags, true)) {
            return '';
        }

        if ($skipNestedLists && ($tag === 'ul' || $tag === 'ol')) {
            return '';
        }

        if ($tag === 'br') {
            return "\n";
        }

        if ($tag === 'img') {
            if (!$this->config->includeImageAltText) {
                return '';
            }
            $alt = trim($node->getAttribute('alt'));
            return $alt !== '' ? '[Image: ' . $this->truncateText($alt, 240) . ']' : '';
        }

        if ($tag === 'code') {
            $text = trim($node->textContent ?? '');
            return $text !== '' ? '`' . $this->truncateText($text, 180) . '`' : '';
        }

        if ($tag === 'a') {
            $label = $this->joinInlinePieces($this->extractChildrenInlinePieces($node, $findings, $stats, $skipNestedLists));
            $href = trim($node->getAttribute('href'));
            if ($href !== '' && $this->isAllowedLinkScheme($href) && $this->config->includeLinks && $stats['links_retained'] < $this->config->maxLinks) {
                $stats['links_retained']++;
                return $label !== '' ? $label . ' (' . $href . ')' : $href;
            }
            return $label;
        }

        return $this->joinInlinePieces($this->extractChildrenInlinePieces($node, $findings, $stats, $skipNestedLists));
    }

    private function extractChildrenInlinePieces(DOMNode $node, array &$findings, array &$stats, bool $skipNestedLists): array
    {
        $pieces = [];
        foreach ($this->nodeListToArray($node->childNodes) as $child) {
            $piece = $this->extractInlineText($child, $findings, $stats, $skipNestedLists);
            if ($piece !== '') {
                $pieces[] = $piece;
            }
        }
        return $pieces;
    }

    private function joinInlinePieces(array $pieces): string
    {
        $out = '';
        foreach ($pieces as $piece) {
            $piece = trim($piece);
            if ($piece === '') {
                continue;
            }
            if (
                $out !== ''
                && !str_ends_with($out, "\n")
                && !preg_match('/[\s(\[{\/-]$/u', $out)
                && !preg_match('/^[,.;:!?%\])}]/u', $piece)
            ) {
                $out .= ' ';
            }
            $out .= $piece;
        }

        $out = preg_replace('/[ \t]+/u', ' ', $out) ?? $out;
        $out = preg_replace('/ ?\n ?/u', "\n", $out) ?? $out;
        $out = preg_replace('/\n{3,}/u', "\n\n", $out) ?? $out;
        return trim($out);
    }

    /** @return HtmlPromptInjectionFinding[] */
    private function findHighEntropyFindings(string $text, string $kind): array
    {
        $findings = [];
        if ($text === '' || !preg_match_all(self::HIGH_ENTROPY_TOKEN_PATTERN, $text, $matches)) {
            return [];
        }

        $seen = [];
        foreach ($matches[0] as $token) {
            $token = trim($token, ".,;:()[]{}<>\"'");
            if ($token === '' || isset($seen[$token])) {
                continue;
            }
            $seen[$token] = true;

            if ($this->stringLength($token) < $this->config->highEntropyMinLength) {
                continue;
            }

            $entropy = $this->entropy($token);
            if ($entropy < 4.15 || count(array_unique(str_split($token))) < 10) {
                continue;
            }

            $findings[] = new HtmlPromptInjectionFinding(
                $kind,
                'medium',
                'Extracted text contains a high-entropy token that looks like a secret, opaque identifier, or payload blob.',
                $this->summarize($token, 180),
                ['entropy' => round($entropy, 3), 'length' => $this->stringLength($token)]
            );
        }

        return $findings;
    }

    private function matchPromptRules(string $text): array
    {
        $matches = [];
        if ($text === '') {
            return $matches;
        }

        foreach (self::PROMPT_RULES as $rule) {
            if (preg_match($rule['regex'], $text)) {
                $matches[] = $rule;
            }
        }

        return $matches;
    }

    private function computeRiskScore(array $findings): float
    {
        $risk = 0.0;
        foreach ($findings as $finding) {
            if (!$finding instanceof HtmlPromptInjectionFinding) {
                continue;
            }
            $weight = $this->severityWeight($finding->severity);
            $risk = 1.0 - ((1.0 - $risk) * (1.0 - $weight));
        }
        return round(min(1.0, $risk), 4);
    }

    private function hasCriticalFinding(array $findings): bool
    {
        foreach ($findings as $finding) {
            if ($finding instanceof HtmlPromptInjectionFinding && $finding->severity === 'critical') {
                return true;
            }
        }
        return false;
    }

    private function severityWeight(string $severity): float
    {
        return match ($severity) {
            'critical' => 0.78,
            'high' => 0.48,
            'medium' => 0.24,
            default => 0.12,
        };
    }

    private function nodeListToArray(DOMNodeList $nodes): array
    {
        $items = [];
        for ($i = 0; $i < $nodes->length; $i++) {
            $node = $nodes->item($i);
            if ($node !== null) {
                $items[] = $node;
            }
        }
        return $items;
    }

    private function isHiddenElement(DOMElement $node): bool
    {
        if ($node->hasAttribute('hidden')) {
            return true;
        }

        if (strtolower(trim($node->getAttribute('aria-hidden'))) === 'true') {
            return true;
        }

        $style = $node->getAttribute('style');
        if ($style !== '' && $this->styleImpliesHidden($style)) {
            return true;
        }

        $class = strtolower($node->getAttribute('class'));
        return str_contains($class, 'sr-only') || str_contains($class, 'visually-hidden');
    }

    private function styleImpliesHidden(string $style): bool
    {
        $style = strtolower($style);
        $needles = [
            'display:none',
            'visibility:hidden',
            'opacity:0',
            'font-size:0',
            'clip-path:inset(100%)',
            'left:-9999',
            'top:-9999',
            'position:absolute;left:-9999',
            'position:fixed;left:-9999',
        ];

        foreach ($needles as $needle) {
            if (str_contains($style, $needle)) {
                return true;
            }
        }

        return false;
    }

    private function isCodeLikeContext(DOMNode $node): bool
    {
        $current = $node;
        while ($current !== null) {
            if ($current instanceof DOMElement) {
                $tag = strtolower($current->tagName);
                if ($tag === 'pre' || $tag === 'code' || $tag === 'samp' || $tag === 'kbd') {
                    return true;
                }
            }
            $current = $current->parentNode;
        }
        return false;
    }

    private function containsControlCharacters(string $value): bool
    {
        return preg_match(self::CONTROL_PATTERN, $value) === 1;
    }

    private function isAllowedLinkScheme(string $value): bool
    {
        if ($value === '' || str_starts_with($value, '#') || str_starts_with($value, '/')) {
            return true;
        }

        $scheme = parse_url($value, PHP_URL_SCHEME);
        if ($scheme === null || $scheme === false) {
            return true;
        }

        return in_array(strtolower($scheme), $this->config->allowedLinkSchemes, true);
    }

    private function entropy(string $value): float
    {
        if ($value === '') {
            return 0.0;
        }

        $counts = [];
        $length = strlen($value);
        foreach (count_chars($value, 1) as $ordinal => $count) {
            $counts[chr($ordinal)] = $count;
        }

        $entropy = 0.0;
        foreach ($counts as $count) {
            $p = $count / $length;
            $entropy -= $p * log($p, 2);
        }

        return $entropy;
    }

    private function truncateText(string $value, int $limit): string
    {
        if ($value === '' || $this->stringLength($value) <= $limit) {
            return $value;
        }

        return rtrim($this->stringSlice($value, max(0, $limit - 18))) . ' [TRUNCATED]';
    }

    private function summarize(string $value, int $limit): string
    {
        $value = $this->normalizeText($value);
        if ($value === '') {
            return '';
        }
        if ($this->stringLength($value) <= $limit) {
            return $value;
        }
        return rtrim($this->stringSlice($value, max(0, $limit - 3))) . '...';
    }

    private function normalizeText(string $value): string
    {
        $value = html_entity_decode($value, ENT_QUOTES | ENT_HTML5, 'UTF-8');
        $value = preg_replace(self::CONTROL_PATTERN, '', $value) ?? $value;
        $value = preg_replace('/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]+/u', ' ', $value) ?? $value;
        $value = preg_replace('/[ \t\r\n]+/u', ' ', $value) ?? $value;
        return trim($value);
    }

    private function stringLength(string $value): int
    {
        return function_exists('mb_strlen') ? mb_strlen($value, 'UTF-8') : strlen($value);
    }

    private function stringSlice(string $value, int $length): string
    {
        if ($length <= 0) {
            return '';
        }

        return function_exists('mb_substr')
            ? mb_substr($value, 0, $length, 'UTF-8')
            : substr($value, 0, $length);
    }
}

final class HtmlPromptInjectionFirewallCli
{
    public static function main(array $argv): int
    {
        $json = false;
        $file = null;
        $sourceUrl = null;
        $maxOutputChars = null;
        $strict = false;

        foreach (array_slice($argv, 1) as $arg) {
            if ($arg === '--json') {
                $json = true;
                continue;
            }
            if ($arg === '--strict-visible') {
                $strict = true;
                continue;
            }
            if ($arg === '--help' || $arg === '-h') {
                self::printUsage($argv[0] ?? 'HtmlPromptInjectionFirewall.php');
                return 0;
            }
            if (str_starts_with($arg, '--file=')) {
                $file = substr($arg, 7);
                continue;
            }
            if (str_starts_with($arg, '--url=')) {
                $sourceUrl = substr($arg, 6);
                continue;
            }
            if (str_starts_with($arg, '--max-output-chars=')) {
                $maxOutputChars = (int) substr($arg, 19);
                continue;
            }

            fwrite(STDERR, "Unknown argument: {$arg}\n");
            self::printUsage($argv[0] ?? 'HtmlPromptInjectionFirewall.php');
            return 1;
        }

        $html = '';
        if ($file !== null) {
            $html = (string) @file_get_contents($file);
            if ($html === '') {
                fwrite(STDERR, "Failed to read file: {$file}\n");
                return 1;
            }
        } else {
            $html = (string) stream_get_contents(STDIN);
        }

        $config = new HtmlPromptInjectionFirewallConfig(
            maxOutputChars: $maxOutputChars ?: 120000,
            dropVisiblePromptLikeText: $strict
        );
        $firewall = new HtmlPromptInjectionFirewall($config);
        $result = $firewall->sanitize($html, $sourceUrl);

        if ($json) {
            echo json_encode($result, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE) . PHP_EOL;
        } else {
            echo $result->text . PHP_EOL;
            fwrite(
                STDERR,
                sprintf(
                    "risk=%.4f needs_review=%s findings=%d output_chars=%d\n",
                    $result->riskScore,
                    $result->needsReview ? 'true' : 'false',
                    count($result->findings),
                    $result->stats['output_chars'] ?? 0
                )
            );
        }

        return 0;
    }

    private static function printUsage(string $script): void
    {
        fwrite(
            STDERR,
            "Usage: php {$script} [--file=path] [--url=https://source.example] [--json] [--max-output-chars=120000] [--strict-visible]\n"
        );
    }
}

if (PHP_SAPI === 'cli' && realpath((string) ($_SERVER['SCRIPT_FILENAME'] ?? '')) === __FILE__) {
    exit(HtmlPromptInjectionFirewallCli::main($argv));
}

/*
This solves prompt injection filtering for HTML before it reaches an LLM, RAG pipeline, agent, crawler, or search index. Built because hidden instructions in comments, off-screen spans, forms, data URLs, and copied site widgets still slip into web-grounded AI systems in 2026, especially in Laravel, WordPress, Symfony, and custom PHP ingestion jobs. Use it when you pull HTML from docs, CMS pages, support portals, tickets, email archives, vendor dashboards, or knowledge bases and you need clean readable text plus a reviewable risk report. The trick: it does two jobs together. First it strips or quarantines hidden and interactive HTML that models should not trust. Then it keeps the human-visible content in a markdown-like text form while scoring suspicious comments, prompt-override phrases, risky link schemes, zero-width control characters, and high-entropy blobs that often hide secrets or opaque payloads. Drop this into a crawler, queue worker, webhook consumer, RAG preprocessor, or middleware layer that turns raw HTML into LLM-ready text. If you are searching GitHub or Google for PHP prompt injection sanitizer, HTML to text firewall for LLM, RAG content sanitizer, hidden prompt detector, or Laravel AI scraping safety, this file is aimed exactly at that problem.
*/