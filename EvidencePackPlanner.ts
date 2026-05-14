import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';

export type EvidenceSourceType =
  | 'web'
  | 'docs'
  | 'code'
  | 'issue'
  | 'paper'
  | 'ticket'
  | 'internal'
  | 'forum'
  | 'dataset'
  | 'spec';

export type EvidenceDropReasonCode =
  | 'blocked'
  | 'empty'
  | 'unsafe'
  | 'duplicate-url'
  | 'duplicate-content'
  | 'near-duplicate'
  | 'low-score'
  | 'budget'
  | 'domain-cap'
  | 'source-cap';

export interface EvidenceCandidate {
  readonly id: string;
  readonly url?: string;
  readonly title?: string;
  readonly snippet?: string;
  readonly content?: string;
  readonly sourceType?: EvidenceSourceType;
  readonly domain?: string;
  readonly similarity?: number;
  readonly publishedAt?: string;
  readonly retrievedAt?: string;
  readonly tags?: ReadonlyArray<string>;
  readonly authoritative?: boolean;
  readonly pinned?: boolean;
  readonly blocked?: boolean;
  readonly metadata?: Record<string, string | number | boolean | null>;
}

export interface EvidencePackOptions {
  readonly maxTotalTokens?: number;
  readonly maxItems?: number;
  readonly maxItemTokens?: number;
  readonly minScore?: number;
  readonly minMarginalScore?: number;
  readonly maxPerDomain?: number;
  readonly maxPerSourceType?: Partial<Record<EvidenceSourceType, number>>;
  readonly duplicateSimilarityThreshold?: number;
  readonly noveltySimilarityThreshold?: number;
  readonly unsafeContentThreshold?: number;
  readonly strictSafety?: boolean;
  readonly freshnessHalfLifeDays?: number;
  readonly preferredDomains?: ReadonlyArray<string>;
  readonly blockedDomains?: ReadonlyArray<string>;
  readonly requiredFacets?: ReadonlyArray<string>;
  readonly stopwords?: ReadonlyArray<string>;
  readonly now?: string | number | Date;
}

export interface EvidenceScoreBreakdown {
  readonly overall: number;
  readonly relevance: number;
  readonly authority: number;
  readonly freshness: number;
  readonly completeness: number;
  readonly safetyPenalty: number;
}

export interface EvidencePlanItem {
  readonly id: string;
  readonly url?: string;
  readonly title: string;
  readonly domain: string;
  readonly sourceType: EvidenceSourceType;
  readonly excerpt: string;
  readonly estimatedTokens: number;
  readonly matchedFacets: ReadonlyArray<string>;
  readonly safetySignals: ReadonlyArray<string>;
  readonly score: number;
  readonly scoreBreakdown: EvidenceScoreBreakdown;
  readonly publishedAt?: string;
  readonly retrievedAt?: string;
}

export interface EvidenceDrop {
  readonly id: string;
  readonly reason: EvidenceDropReasonCode;
  readonly message: string;
  readonly replacedById?: string;
  readonly score?: number;
}

export interface EvidencePlanStats {
  readonly inputItems: number;
  readonly filteredItems: number;
  readonly selectedItems: number;
  readonly droppedItems: number;
  readonly totalSelectedTokens: number;
  readonly duplicatesRemoved: number;
  readonly unsafeItemsRemoved: number;
  readonly domainSpread: number;
  readonly sourceSpread: number;
}

export interface EvidencePackPlan {
  readonly query: string;
  readonly selected: ReadonlyArray<EvidencePlanItem>;
  readonly dropped: ReadonlyArray<EvidenceDrop>;
  readonly uncoveredFacets: ReadonlyArray<string>;
  readonly warnings: ReadonlyArray<string>;
  readonly stats: EvidencePlanStats;
}

interface NormalizedOptions {
  readonly maxTotalTokens: number;
  readonly maxItems: number;
  readonly maxItemTokens: number;
  readonly minScore: number;
  readonly minMarginalScore: number;
  readonly maxPerDomain: number;
  readonly maxPerSourceType: Record<EvidenceSourceType, number>;
  readonly duplicateSimilarityThreshold: number;
  readonly noveltySimilarityThreshold: number;
  readonly unsafeContentThreshold: number;
  readonly strictSafety: boolean;
  readonly freshnessHalfLifeDays: number;
  readonly preferredDomains: string[];
  readonly blockedDomains: Set<string>;
  readonly requiredFacets: string[];
  readonly stopwords: Set<string>;
  readonly nowMs: number;
}

interface PreparedCandidate {
  readonly candidate: EvidenceCandidate;
  readonly sourceType: EvidenceSourceType;
  readonly canonicalUrl?: string;
  readonly domain: string;
  readonly title: string;
  readonly rawText: string;
  readonly normalizedText: string;
  readonly excerpt: string;
  readonly excerptTokens: number;
  readonly contentTokens: number;
  readonly textFingerprint: string;
  readonly shingles: Set<string>;
  readonly matchedFacets: string[];
  readonly lexicalOverlap: number;
  readonly injectionScore: number;
  readonly safetySignals: string[];
  readonly freshnessScore: number;
  readonly scoreBreakdown: EvidenceScoreBreakdown;
  readonly baseScore: number;
}

interface InjectionSignal {
  readonly label: string;
  readonly regex: RegExp;
  readonly weight: number;
}

const DEFAULT_SOURCE_TYPE_LIMITS: Record<EvidenceSourceType, number> = {
  web: 4,
  docs: 4,
  code: 4,
  issue: 3,
  paper: 3,
  ticket: 2,
  internal: 4,
  forum: 2,
  dataset: 2,
  spec: 3,
};

const DEFAULT_SOURCE_WEIGHTS: Record<EvidenceSourceType, number> = {
  spec: 0.96,
  docs: 0.92,
  paper: 0.9,
  dataset: 0.88,
  code: 0.86,
  internal: 0.82,
  issue: 0.74,
  web: 0.7,
  ticket: 0.66,
  forum: 0.58,
};

const SOURCE_TYPE_FRESHNESS_MULTIPLIER: Record<EvidenceSourceType, number> = {
  web: 1,
  docs: 0.55,
  code: 0.35,
  issue: 0.9,
  paper: 0.18,
  ticket: 0.95,
  internal: 0.85,
  forum: 0.9,
  dataset: 0.4,
  spec: 0.3,
};

const DEFAULT_STOPWORDS = new Set([
  'a',
  'an',
  'and',
  'are',
  'as',
  'at',
  'be',
  'by',
  'for',
  'from',
  'how',
  'in',
  'into',
  'is',
  'it',
  'of',
  'on',
  'or',
  'that',
  'the',
  'their',
  'this',
  'to',
  'use',
  'using',
  'what',
  'when',
  'why',
  'with',
]);

const DEFAULT_INJECTION_SIGNALS: ReadonlyArray<InjectionSignal> = [
  {
    label: 'ignore-instructions',
    regex: /\bignore\s+(?:all|any|the|previous|earlier)\s+instructions?\b/i,
    weight: 0.22,
  },
  {
    label: 'system-prompt',
    regex: /\b(?:system|developer)\s+prompt\b/i,
    weight: 0.18,
  },
  {
    label: 'role-json',
    regex: /"role"\s*:\s*"(?:system|developer|assistant)"/i,
    weight: 0.18,
  },
  {
    label: 'tool-call',
    regex: /\btool\s+call\b|\bfunction\s+call\b/i,
    weight: 0.15,
  },
  {
    label: 'hidden-prompt',
    regex: /\bhidden\s+prompt\b|\bprompt\s+leak\b|\breveal\s+your\s+prompt\b/i,
    weight: 0.2,
  },
  {
    label: 'override-policy',
    regex: /\boverride\b.{0,32}\bpolicy\b|\bdisable\b.{0,32}\bguardrails?\b/i,
    weight: 0.16,
  },
  {
    label: 'secret-exfiltration',
    regex: /\bexfiltrat(?:e|ion)\b|\bsteal\b.{0,20}\btoken\b|\bcopy\b.{0,20}\bapi key\b/i,
    weight: 0.24,
  },
  {
    label: 'model-self-reference',
    regex: /\byou are chatgpt\b|\byou are claude\b|\byou are an ai assistant\b/i,
    weight: 0.12,
  },
];

export function planEvidencePack(
  query: string,
  candidates: ReadonlyArray<EvidenceCandidate>,
  options: EvidencePackOptions = {}
): EvidencePackPlan {
  if (!query || !query.trim()) {
    throw new Error('query must be a non-empty string');
  }

  const normalizedOptions = normalizeOptions(options);
  const facets = buildFacets(query, normalizedOptions);
  const dropped: EvidenceDrop[] = [];

  const prepared: PreparedCandidate[] = [];
  for (const candidate of candidates) {
    const outcome = prepareCandidate(candidate, query, facets, normalizedOptions);
    if ('drop' in outcome) {
      dropped.push(outcome.drop);
      continue;
    }
    prepared.push(outcome.candidate);
  }

  const sortedPrepared = stableSortPrepared(prepared);
  const deduped = dedupeCandidates(sortedPrepared, dropped, normalizedOptions);
  const selectedPrepared = selectCandidates(deduped, facets, normalizedOptions, dropped);

  const selected = selectedPrepared.map((candidate) => toPlanItem(candidate));
  const totalSelectedTokens = selected.reduce((sum, item) => sum + item.estimatedTokens, 0);
  const uncoveredFacets = facets.filter(
    (facet) => !selected.some((item) => item.matchedFacets.includes(facet))
  );
  const warnings = buildWarnings(selected, uncoveredFacets, dropped);

  return {
    query,
    selected,
    dropped,
    uncoveredFacets,
    warnings,
    stats: {
      inputItems: candidates.length,
      filteredItems: deduped.length,
      selectedItems: selected.length,
      droppedItems: dropped.length,
      totalSelectedTokens,
      duplicatesRemoved: dropped.filter(
        (item) =>
          item.reason === 'duplicate-url' ||
          item.reason === 'duplicate-content' ||
          item.reason === 'near-duplicate'
      ).length,
      unsafeItemsRemoved: dropped.filter((item) => item.reason === 'unsafe').length,
      domainSpread: new Set(selected.map((item) => item.domain)).size,
      sourceSpread: new Set(selected.map((item) => item.sourceType)).size,
    },
  };
}

function normalizeOptions(options: EvidencePackOptions): NormalizedOptions {
  const preferredDomains = normalizeDomainList(options.preferredDomains ?? []);
  const blockedDomains = new Set(normalizeDomainList(options.blockedDomains ?? []));
  const stopwords = new Set(DEFAULT_STOPWORDS);
  for (const word of options.stopwords ?? []) {
    if (word.trim()) {
      stopwords.add(word.trim().toLowerCase());
    }
  }

  return {
    maxTotalTokens: coercePositiveInteger(options.maxTotalTokens, 2200),
    maxItems: coercePositiveInteger(options.maxItems, 10),
    maxItemTokens: coercePositiveInteger(options.maxItemTokens, 260),
    minScore: clamp(options.minScore ?? 0.28, 0, 1),
    minMarginalScore: clamp(options.minMarginalScore ?? 0.34, 0, 2),
    maxPerDomain: coercePositiveInteger(options.maxPerDomain, 2),
    maxPerSourceType: {
      ...DEFAULT_SOURCE_TYPE_LIMITS,
      ...(options.maxPerSourceType ?? {}),
    },
    duplicateSimilarityThreshold: clamp(options.duplicateSimilarityThreshold ?? 0.92, 0.5, 1),
    noveltySimilarityThreshold: clamp(options.noveltySimilarityThreshold ?? 0.82, 0.3, 1),
    unsafeContentThreshold: clamp(options.unsafeContentThreshold ?? 0.4, 0.2, 1),
    strictSafety: options.strictSafety ?? true,
    freshnessHalfLifeDays: coercePositiveInteger(options.freshnessHalfLifeDays, 45),
    preferredDomains,
    blockedDomains,
    requiredFacets: dedupeStrings(options.requiredFacets ?? []),
    stopwords,
    nowMs: normalizeDateLike(options.now) ?? Date.now(),
  };
}

function prepareCandidate(
  candidate: EvidenceCandidate,
  query: string,
  facets: ReadonlyArray<string>,
  options: NormalizedOptions
): { candidate: PreparedCandidate } | { drop: EvidenceDrop } {
  if (!candidate.id || !candidate.id.trim()) {
    return {
      drop: {
        id: '',
        reason: 'empty',
        message: 'Candidate is missing a stable id.',
      },
    };
  }

  if (candidate.blocked) {
    return {
      drop: {
        id: candidate.id,
        reason: 'blocked',
        message: 'Candidate was blocked by upstream policy.',
      },
    };
  }

  const sourceType = candidate.sourceType ?? inferSourceType(candidate);
  const canonicalUrl = canonicalizeUrl(candidate.url);
  const domain = resolveDomain(candidate.domain, canonicalUrl);
  if (domain && matchesDomainPolicy(domain, options.blockedDomains)) {
    return {
      drop: {
        id: candidate.id,
        reason: 'blocked',
        message: `Domain ${domain} is blocked by policy.`,
      },
    };
  }

  const rawText = buildRawText(candidate);
  const normalizedText = normalizeContentText(rawText);
  if (!normalizedText) {
    return {
      drop: {
        id: candidate.id,
        reason: 'empty',
        message: 'Candidate does not contain usable text after normalization.',
      },
    };
  }

  const safety = detectInjectionSignals(rawText);
  if (options.strictSafety && safety.score >= options.unsafeContentThreshold) {
    return {
      drop: {
        id: candidate.id,
        reason: 'unsafe',
        message: `Candidate matched unsafe retrieval patterns: ${safety.signals.join(', ')}`,
      },
    };
  }

  const matchedFacets = matchFacets(candidate, rawText, facets);
  const excerpt = buildEvidenceExcerpt(rawText, matchedFacets, options.maxItemTokens);
  const excerptTokens = estimateTokens(excerpt);
  const contentTokens = estimateTokens(rawText);
  const lexicalOverlap = computeLexicalOverlap(query, rawText, facets, options.stopwords);
  const freshnessScore = computeFreshnessScore(candidate, sourceType, options);
  const scoreBreakdown = scoreCandidate(
    candidate,
    sourceType,
    domain,
    lexicalOverlap,
    matchedFacets,
    safety.score,
    freshnessScore,
    contentTokens,
    options
  );

  if (!candidate.pinned && scoreBreakdown.overall < options.minScore) {
    return {
      drop: {
        id: candidate.id,
        reason: 'low-score',
        message: `Candidate scored ${scoreBreakdown.overall.toFixed(3)}, below the ${options.minScore.toFixed(3)} threshold.`,
        score: scoreBreakdown.overall,
      },
    };
  }

  return {
    candidate: {
      candidate,
      sourceType,
      canonicalUrl,
      domain,
      title: normalizeTitle(candidate.title, rawText),
      rawText,
      normalizedText,
      excerpt,
      excerptTokens,
      contentTokens,
      textFingerprint: hash(normalizedText),
      shingles: buildShingles(normalizedText, 3),
      matchedFacets,
      lexicalOverlap,
      injectionScore: safety.score,
      safetySignals: safety.signals,
      freshnessScore,
      scoreBreakdown,
      baseScore: scoreBreakdown.overall,
    },
  };
}

function stableSortPrepared(candidates: ReadonlyArray<PreparedCandidate>): PreparedCandidate[] {
  return [...candidates].sort((left, right) => {
    if (left.candidate.pinned !== right.candidate.pinned) {
      return left.candidate.pinned ? -1 : 1;
    }
    if ((left.candidate.authoritative ?? false) !== (right.candidate.authoritative ?? false)) {
      return left.candidate.authoritative ? -1 : 1;
    }
    if (left.baseScore !== right.baseScore) {
      return right.baseScore - left.baseScore;
    }
    return left.candidate.id.localeCompare(right.candidate.id);
  });
}

function dedupeCandidates(
  candidates: ReadonlyArray<PreparedCandidate>,
  dropped: EvidenceDrop[],
  options: NormalizedOptions
): PreparedCandidate[] {
  const kept: PreparedCandidate[] = [];
  const urlIndex = new Map<string, PreparedCandidate>();
  const contentIndex = new Map<string, PreparedCandidate>();

  for (const candidate of candidates) {
    if (candidate.canonicalUrl) {
      const duplicate = urlIndex.get(candidate.canonicalUrl);
      if (duplicate) {
        dropped.push({
          id: candidate.candidate.id,
          reason: 'duplicate-url',
          message: `Same canonical URL as ${duplicate.candidate.id}.`,
          replacedById: duplicate.candidate.id,
          score: candidate.baseScore,
        });
        continue;
      }
    }

    const exactContentDuplicate = contentIndex.get(candidate.textFingerprint);
    if (exactContentDuplicate) {
      dropped.push({
        id: candidate.candidate.id,
        reason: 'duplicate-content',
        message: `Same normalized content as ${exactContentDuplicate.candidate.id}.`,
        replacedById: exactContentDuplicate.candidate.id,
        score: candidate.baseScore,
      });
      continue;
    }

    const nearDuplicate = kept.find((existing) => {
      if (existing.domain !== candidate.domain && existing.sourceType !== candidate.sourceType) {
        return false;
      }
      return jaccardSimilarity(existing.shingles, candidate.shingles) >= options.duplicateSimilarityThreshold;
    });

    if (nearDuplicate) {
      dropped.push({
        id: candidate.candidate.id,
        reason: 'near-duplicate',
        message: `High-overlap evidence already kept as ${nearDuplicate.candidate.id}.`,
        replacedById: nearDuplicate.candidate.id,
        score: candidate.baseScore,
      });
      continue;
    }

    kept.push(candidate);
    if (candidate.canonicalUrl) {
      urlIndex.set(candidate.canonicalUrl, candidate);
    }
    contentIndex.set(candidate.textFingerprint, candidate);
  }

  return kept;
}

function selectCandidates(
  candidates: ReadonlyArray<PreparedCandidate>,
  facets: ReadonlyArray<string>,
  options: NormalizedOptions,
  dropped: EvidenceDrop[]
): PreparedCandidate[] {
  const selected: PreparedCandidate[] = [];
  const selectedIds = new Set<string>();
  const domainCounts = new Map<string, number>();
  const sourceCounts = new Map<EvidenceSourceType, number>();
  let remainingTokens = options.maxTotalTokens;

  const trySelect = (candidate: PreparedCandidate): boolean => {
    if (selectedIds.has(candidate.candidate.id) || selected.length >= options.maxItems) {
      return false;
    }
    const domainCount = getMapCount(domainCounts, candidate.domain);
    if (domainCount >= options.maxPerDomain) {
      return false;
    }
    const sourceCount = getMapCount(sourceCounts, candidate.sourceType);
    if (sourceCount >= options.maxPerSourceType[candidate.sourceType]) {
      return false;
    }
    if (candidate.excerptTokens > remainingTokens) {
      return false;
    }
    selected.push(candidate);
    selectedIds.add(candidate.candidate.id);
    domainCounts.set(candidate.domain, domainCount + 1);
    sourceCounts.set(candidate.sourceType, sourceCount + 1);
    remainingTokens -= candidate.excerptTokens;
    return true;
  };

  for (const candidate of candidates) {
    if (candidate.candidate.pinned) {
      trySelect(candidate);
    }
  }

  const uncovered = new Set(facets);
  for (const item of selected) {
    for (const facet of item.matchedFacets) {
      uncovered.delete(facet);
    }
  }

  while (uncovered.size > 0 && selected.length < options.maxItems) {
    let bestCandidate: PreparedCandidate | undefined;
    let bestMarginal = -Infinity;
    for (const candidate of candidates) {
      if (selectedIds.has(candidate.candidate.id)) {
        continue;
      }
      const marginal = computeMarginalUtility(candidate, selected, uncovered, remainingTokens, options);
      if (marginal > bestMarginal) {
        bestMarginal = marginal;
        bestCandidate = candidate;
      }
    }
    if (!bestCandidate || bestMarginal < options.minMarginalScore || !trySelect(bestCandidate)) {
      break;
    }
    for (const facet of bestCandidate.matchedFacets) {
      uncovered.delete(facet);
    }
  }

  while (selected.length < options.maxItems) {
    let bestCandidate: PreparedCandidate | undefined;
    let bestMarginal = -Infinity;
    for (const candidate of candidates) {
      if (selectedIds.has(candidate.candidate.id)) {
        continue;
      }
      const marginal = computeMarginalUtility(candidate, selected, uncovered, remainingTokens, options);
      if (marginal > bestMarginal) {
        bestMarginal = marginal;
        bestCandidate = candidate;
      }
    }
    if (!bestCandidate || bestMarginal < options.minMarginalScore || !trySelect(bestCandidate)) {
      break;
    }
    for (const facet of bestCandidate.matchedFacets) {
      uncovered.delete(facet);
    }
  }

  for (const candidate of candidates) {
    if (selectedIds.has(candidate.candidate.id)) {
      continue;
    }
    const domainCount = getMapCount(domainCounts, candidate.domain);
    const sourceCount = getMapCount(sourceCounts, candidate.sourceType);
    if (candidate.excerptTokens > remainingTokens) {
      dropped.push({
        id: candidate.candidate.id,
        reason: 'budget',
        message: `Not enough remaining token budget to include ${candidate.excerptTokens} more tokens.`,
        score: candidate.baseScore,
      });
    } else if (domainCount >= options.maxPerDomain) {
      dropped.push({
        id: candidate.candidate.id,
        reason: 'domain-cap',
        message: `Domain cap reached for ${candidate.domain}.`,
        score: candidate.baseScore,
      });
    } else if (sourceCount >= options.maxPerSourceType[candidate.sourceType]) {
      dropped.push({
        id: candidate.candidate.id,
        reason: 'source-cap',
        message: `Source cap reached for ${candidate.sourceType}.`,
        score: candidate.baseScore,
      });
    } else {
      dropped.push({
        id: candidate.candidate.id,
        reason: 'low-score',
        message: `Marginal utility stayed below ${options.minMarginalScore.toFixed(3)} after higher-value evidence was selected.`,
        score: candidate.baseScore,
      });
    }
  }

  return selected;
}

function computeMarginalUtility(
  candidate: PreparedCandidate,
  selected: ReadonlyArray<PreparedCandidate>,
  uncovered: ReadonlySet<string>,
  remainingTokens: number,
  options: NormalizedOptions
): number {
  if (candidate.excerptTokens > remainingTokens) {
    return -Infinity;
  }

  const coverageGain =
    candidate.matchedFacets.filter((facet) => uncovered.has(facet)).length / Math.max(uncovered.size, 1);
  const domainFreshnessBoost = selected.some((item) => item.domain === candidate.domain) ? 0 : 0.06;
  const sourceDiversityBoost = selected.some((item) => item.sourceType === candidate.sourceType) ? 0 : 0.04;
  const preferredDomainBoost = matchesDomainPolicy(candidate.domain, options.preferredDomains) ? 0.05 : 0;
  const pinnedBoost = candidate.candidate.pinned ? 0.12 : 0;
  const maxSimilarity = selected.reduce((highest, item) => {
    return Math.max(highest, jaccardSimilarity(item.shingles, candidate.shingles));
  }, 0);
  const noveltyPenalty =
    maxSimilarity >= options.noveltySimilarityThreshold
      ? (maxSimilarity - options.noveltySimilarityThreshold) * 0.65
      : 0;
  const sizePenalty = candidate.excerptTokens / Math.max(remainingTokens, candidate.excerptTokens) * 0.05;

  return (
    candidate.baseScore +
    coverageGain * 0.2 +
    domainFreshnessBoost +
    sourceDiversityBoost +
    preferredDomainBoost +
    pinnedBoost -
    noveltyPenalty -
    sizePenalty
  );
}

function scoreCandidate(
  candidate: EvidenceCandidate,
  sourceType: EvidenceSourceType,
  domain: string,
  lexicalOverlap: number,
  matchedFacets: ReadonlyArray<string>,
  injectionScore: number,
  freshnessScore: number,
  contentTokens: number,
  options: NormalizedOptions
): EvidenceScoreBreakdown {
  const retrievalSimilarity = clamp(candidate.similarity ?? lexicalOverlap, 0, 1);
  const facetCoverage = matchedFacets.length === 0 ? 0 : clamp(matchedFacets.length / 6, 0, 1);
  const titleBoost = candidate.title && matchedFacets.length > 0 ? 0.08 : 0;
  const relevance = clamp(retrievalSimilarity * 0.6 + lexicalOverlap * 0.28 + facetCoverage * 0.12 + titleBoost, 0, 1);

  let authority = DEFAULT_SOURCE_WEIGHTS[sourceType];
  if (candidate.authoritative) {
    authority += 0.08;
  }
  if (domain.endsWith('.gov') || domain.endsWith('.edu')) {
    authority += 0.05;
  }
  if (matchesDomainPolicy(domain, options.preferredDomains)) {
    authority += 0.08;
  }
  if (domain.includes('docs.') || domain.includes('.docs')) {
    authority += 0.03;
  }
  authority = clamp(authority, 0, 1);

  const completeness = clamp(
    1 - Math.min(Math.abs(Math.log(Math.max(contentTokens, 1)) - Math.log(180)) / 3, 1),
    0,
    1
  );

  const safetyPenalty = clamp(injectionScore, 0, 1);
  const overall = clamp(
    relevance * 0.5 +
      authority * 0.22 +
      freshnessScore * 0.15 +
      completeness * 0.13 -
      safetyPenalty * 0.32,
    0,
    1
  );

  return {
    overall,
    relevance,
    authority,
    freshness: freshnessScore,
    completeness,
    safetyPenalty,
  };
}

function computeFreshnessScore(
  candidate: EvidenceCandidate,
  sourceType: EvidenceSourceType,
  options: NormalizedOptions
): number {
  const timestamp =
    normalizeDateLike(candidate.publishedAt) ??
    normalizeDateLike(candidate.retrievedAt);
  if (!timestamp) {
    return 0.58;
  }

  const ageDays = Math.max(0, (options.nowMs - timestamp) / 86_400_000);
  const multiplier = SOURCE_TYPE_FRESHNESS_MULTIPLIER[sourceType];
  const scaledHalfLife = Math.max(1, options.freshnessHalfLifeDays / Math.max(multiplier, 0.1));
  const score = Math.exp((-Math.log(2) * ageDays) / scaledHalfLife);
  return clamp(score, 0, 1);
}

function buildFacets(query: string, options: NormalizedOptions): string[] {
  const required = dedupeStrings(options.requiredFacets);
  if (required.length > 0) {
    return required;
  }

  const terms = tokenize(normalizeContentText(query));
  const facets: string[] = [];
  for (const term of terms) {
    if (term.length < 4 || options.stopwords.has(term)) {
      continue;
    }
    facets.push(term);
    if (facets.length >= 8) {
      break;
    }
  }

  return dedupeStrings(facets);
}

function matchFacets(
  candidate: EvidenceCandidate,
  rawText: string,
  facets: ReadonlyArray<string>
): string[] {
  const haystack = normalizeContentText(
    [candidate.title ?? '', rawText, ...(candidate.tags ?? [])].join('\n')
  );
  return facets.filter((facet) => haystack.includes(facet.toLowerCase()));
}

function computeLexicalOverlap(
  query: string,
  text: string,
  facets: ReadonlyArray<string>,
  stopwords: ReadonlySet<string>
): number {
  const queryTerms = new Set(
    tokenize(normalizeContentText(query)).filter((term) => term.length >= 3 && !stopwords.has(term))
  );
  if (queryTerms.size === 0) {
    return 0;
  }

  const normalizedText = normalizeContentText(text);
  const textTerms = new Set(tokenize(normalizedText));
  let matchedTerms = 0;
  for (const term of queryTerms) {
    if (textTerms.has(term)) {
      matchedTerms += 1;
    }
  }

  const facetHits = facets.filter((facet) => normalizedText.includes(facet)).length;
  const termScore = matchedTerms / queryTerms.size;
  const facetScore = facets.length === 0 ? 0 : facetHits / facets.length;
  return clamp(termScore * 0.7 + facetScore * 0.3, 0, 1);
}

function buildEvidenceExcerpt(text: string, facets: ReadonlyArray<string>, maxTokens: number): string {
  const normalized = normalizeWhitespace(text);
  if (!normalized) {
    return '';
  }

  const maxChars = Math.max(240, maxTokens * 4);
  const sentences = splitSentences(normalized);
  if (sentences.length === 0) {
    return truncateWithEllipsis(normalized, maxChars);
  }

  const chosen: string[] = [];
  const remainingFacets = new Set(facets.map((facet) => facet.toLowerCase()));

  for (const sentence of sentences) {
    const lower = sentence.toLowerCase();
    const matchesFacet = [...remainingFacets].some((facet) => lower.includes(facet));
    if (!matchesFacet) {
      continue;
    }
    chosen.push(sentence);
    for (const facet of [...remainingFacets]) {
      if (lower.includes(facet)) {
        remainingFacets.delete(facet);
      }
    }
    if (chosen.join(' ').length >= maxChars * 0.75 || remainingFacets.size === 0) {
      break;
    }
  }

  if (chosen.length === 0) {
    return truncateWithEllipsis(sentences.slice(0, 3).join(' '), maxChars);
  }

  const excerpt = chosen.join(' ');
  if (excerpt.length >= maxChars) {
    return truncateWithEllipsis(excerpt, maxChars);
  }

  const fallbackTail = sentences.find((sentence) => !chosen.includes(sentence));
  return truncateWithEllipsis([excerpt, fallbackTail].filter(Boolean).join(' '), maxChars);
}

function detectInjectionSignals(text: string): { score: number; signals: string[] } {
  const signals: string[] = [];
  let score = 0;
  for (const signal of DEFAULT_INJECTION_SIGNALS) {
    if (!signal.regex.test(text)) {
      continue;
    }
    signals.push(signal.label);
    score += signal.weight;
  }
  return {
    score: clamp(score, 0, 1),
    signals,
  };
}

function canonicalizeUrl(url?: string): string | undefined {
  if (!url) {
    return undefined;
  }

  try {
    const parsed = new URL(url);
    parsed.hash = '';
    if ((parsed.protocol === 'https:' && parsed.port === '443') || (parsed.protocol === 'http:' && parsed.port === '80')) {
      parsed.port = '';
    }
    for (const key of [...parsed.searchParams.keys()]) {
      if (
        key.startsWith('utm_') ||
        key === 'gclid' ||
        key === 'fbclid' ||
        key === 'ref' ||
        key === 'ref_src' ||
        key === 'source'
      ) {
        parsed.searchParams.delete(key);
      }
    }
    parsed.hostname = parsed.hostname.toLowerCase();
    parsed.pathname = parsed.pathname
      .replace(/\/index\.(html?|php|aspx?)$/i, '/')
      .replace(/\/{2,}/g, '/');
    if (parsed.pathname.length > 1 && parsed.pathname.endsWith('/')) {
      parsed.pathname = parsed.pathname.slice(0, -1);
    }
    return parsed.toString();
  } catch {
    return url.trim() || undefined;
  }
}

function resolveDomain(inputDomain: string | undefined, canonicalUrl: string | undefined): string {
  if (inputDomain?.trim()) {
    return normalizeDomain(inputDomain);
  }
  if (!canonicalUrl) {
    return 'unknown';
  }
  try {
    return normalizeDomain(new URL(canonicalUrl).hostname);
  } catch {
    return 'unknown';
  }
}

function normalizeDomain(domain: string): string {
  return domain.trim().toLowerCase().replace(/^www\./, '');
}

function normalizeDomainList(domains: ReadonlyArray<string>): string[] {
  return dedupeStrings(domains.map((domain) => normalizeDomain(domain)));
}

function matchesDomainPolicy(domain: string, domains: Iterable<string>): boolean {
  for (const candidate of domains) {
    if (domain === candidate || domain.endsWith(`.${candidate}`)) {
      return true;
    }
  }
  return false;
}

function normalizeTitle(title: string | undefined, text: string): string {
  if (title && title.trim()) {
    return normalizeWhitespace(title);
  }
  const firstSentence = splitSentences(normalizeWhitespace(text))[0] ?? 'Untitled evidence';
  return truncateWithEllipsis(firstSentence, 120);
}

function inferSourceType(candidate: EvidenceCandidate): EvidenceSourceType {
  const url = candidate.url?.toLowerCase() ?? '';
  if (url.includes('/docs/') || url.includes('docs.')) {
    return 'docs';
  }
  if (url.includes('/issues/') || url.includes('/pull/')) {
    return 'issue';
  }
  if (url.includes('/blob/') || url.endsWith('.ts') || url.endsWith('.py')) {
    return 'code';
  }
  if (url.includes('arxiv.org') || url.includes('paperswithcode')) {
    return 'paper';
  }
  return 'web';
}

function buildRawText(candidate: EvidenceCandidate): string {
  return [candidate.title ?? '', candidate.snippet ?? '', candidate.content ?? '', ...(candidate.tags ?? [])]
    .map((value) => normalizeWhitespace(String(value)))
    .filter(Boolean)
    .join('\n');
}

function normalizeContentText(text: string): string {
  return text
    .normalize('NFKC')
    .toLowerCase()
    .replace(/[`*_#>\[\]\(\)]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeWhitespace(text: string): string {
  return text.replace(/\s+/g, ' ').trim();
}

function tokenize(text: string): string[] {
  return text
    .split(/[^a-z0-9]+/i)
    .map((part) => part.trim().toLowerCase())
    .filter(Boolean);
}

function splitSentences(text: string): string[] {
  return text
    .split(/(?<=[.!?])\s+/)
    .map((sentence) => sentence.trim())
    .filter(Boolean);
}

function truncateWithEllipsis(text: string, maxChars: number): string {
  if (text.length <= maxChars) {
    return text;
  }
  return `${text.slice(0, Math.max(0, maxChars - 1)).trimEnd()}...`;
}

function buildShingles(text: string, width: number): Set<string> {
  const tokens = tokenize(text);
  const shingles = new Set<string>();
  if (tokens.length === 0) {
    return shingles;
  }
  if (tokens.length < width) {
    shingles.add(tokens.join(' '));
    return shingles;
  }
  for (let index = 0; index <= tokens.length - width; index += 1) {
    shingles.add(tokens.slice(index, index + width).join(' '));
  }
  return shingles;
}

function jaccardSimilarity(left: ReadonlySet<string>, right: ReadonlySet<string>): number {
  if (left.size === 0 || right.size === 0) {
    return 0;
  }
  let intersection = 0;
  for (const item of left) {
    if (right.has(item)) {
      intersection += 1;
    }
  }
  const union = left.size + right.size - intersection;
  return union === 0 ? 0 : intersection / union;
}

function estimateTokens(text: string): number {
  if (!text) {
    return 0;
  }
  const words = text.trim().split(/\s+/).length;
  return Math.max(1, Math.ceil(text.length / 4), Math.ceil(words * 1.15));
}

function buildWarnings(selected: ReadonlyArray<EvidencePlanItem>, uncoveredFacets: ReadonlyArray<string>, dropped: ReadonlyArray<EvidenceDrop>): string[] {
  const warnings: string[] = [];
  if (uncoveredFacets.length > 0) {
    warnings.push(`Uncovered query facets: ${uncoveredFacets.join(', ')}`);
  }
  if (selected.length > 0) {
    const domainCounts = new Map<string, number>();
    for (const item of selected) {
      domainCounts.set(item.domain, getMapCount(domainCounts, item.domain) + 1);
    }
    const dominant = [...domainCounts.entries()].sort((left, right) => right[1] - left[1])[0];
    if (dominant && dominant[1] > Math.ceil(selected.length / 2)) {
      warnings.push(`Evidence is dominated by ${dominant[0]} (${dominant[1]} of ${selected.length} items).`);
    }
  }
  const unsafeRemoved = dropped.filter((item) => item.reason === 'unsafe').length;
  if (unsafeRemoved > 0) {
    warnings.push(`Excluded ${unsafeRemoved} candidate${unsafeRemoved === 1 ? '' : 's'} for prompt-injection or unsafe retrieval patterns.`);
  }
  return warnings;
}

function toPlanItem(candidate: PreparedCandidate): EvidencePlanItem {
  return {
    id: candidate.candidate.id,
    url: candidate.canonicalUrl,
    title: candidate.title,
    domain: candidate.domain,
    sourceType: candidate.sourceType,
    excerpt: candidate.excerpt,
    estimatedTokens: candidate.excerptTokens,
    matchedFacets: candidate.matchedFacets,
    safetySignals: candidate.safetySignals,
    score: candidate.baseScore,
    scoreBreakdown: candidate.scoreBreakdown,
    publishedAt: candidate.candidate.publishedAt,
    retrievedAt: candidate.candidate.retrievedAt,
  };
}

function hash(value: string): string {
  return createHash('sha256').update(value).digest('hex');
}

function normalizeDateLike(value: string | number | Date | undefined): number | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : undefined;
  }
  if (value instanceof Date) {
    return Number.isFinite(value.getTime()) ? value.getTime() : undefined;
  }
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(Math.max(value, min), max);
}

function coercePositiveInteger(value: number | undefined, fallback: number): number {
  if (typeof value !== 'number' || !Number.isFinite(value) || value <= 0) {
    return fallback;
  }
  return Math.floor(value);
}

function dedupeStrings(values: ReadonlyArray<string>): string[] {
  const seen = new Set<string>();
  const output: string[] = [];
  for (const value of values) {
    const normalized = value.trim().toLowerCase();
    if (!normalized || seen.has(normalized)) {
      continue;
    }
    seen.add(normalized);
    output.push(normalized);
  }
  return output;
}

function getMapCount<TKey>(map: ReadonlyMap<TKey, number>, key: TKey): number {
  return map.get(key) ?? 0;
}

interface CliInput {
  readonly query: string;
  readonly items: EvidenceCandidate[];
  readonly options?: EvidencePackOptions;
}

async function runCli(): Promise<void> {
  const args = process.argv.slice(2);
  if (args.length < 1 || args.length > 2) {
    console.error('Usage: tsx EvidencePackPlanner.ts <input.json> [maxTotalTokens]');
    process.exitCode = 1;
    return;
  }

  const [inputPath, maxTotalTokensArg] = args;
  const raw = await readFile(inputPath, 'utf8');
  const parsed = JSON.parse(raw) as CliInput;
  const cliOptions: EvidencePackOptions = { ...(parsed.options ?? {}) };

  if (maxTotalTokensArg !== undefined) {
    const maxTotalTokens = Number.parseInt(maxTotalTokensArg, 10);
    if (!Number.isFinite(maxTotalTokens) || maxTotalTokens <= 0) {
      throw new Error(`Invalid maxTotalTokens value: ${maxTotalTokensArg}`);
    }
    cliOptions.maxTotalTokens = maxTotalTokens;
  }

  const plan = planEvidencePack(parsed.query, parsed.items, cliOptions);
  process.stdout.write(`${JSON.stringify(plan, null, 2)}\n`);
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
This solves evidence packing for RAG, AI search, citation planning, retrieval deduplication, context budget management, and prompt injection filtering in one TypeScript file. Built because April 2026 retrieval stacks still fail in the same boring way: they find too many snippets, they overpack duplicates from the same domain, they let prompt-like junk from scraped pages leak into the model, and they waste expensive context on evidence that looks relevant but is stale, thin, or low trust.

Use it when you have search results, vector hits, docs chunks, issues, code snippets, tickets, or internal notes and need to turn that noisy pile into a compact evidence set that a model can safely read. The trick: it does not just rank by one score. It normalizes URLs, removes exact and near duplicates, scores authority and freshness, checks for instruction-shaped garbage in retrieved text, protects domain diversity, and then greedily packs the best mix under a token budget while still trying to cover the important parts of the query.

Drop this into any Node or TypeScript agent, RAG service, search backend, research assistant, eval harness, or citation pipeline where you want better evidence selection before calling an LLM. I kept it as one file on purpose so Pavan can fork it fast, audit every rule, tune the scoring, and wire it into existing OpenAI, Anthropic, Vercel AI SDK, LangGraph, custom MCP, or internal retrieval systems without first untangling a big package or framework.
*/
