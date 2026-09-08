# Dynamic Token Prioritizer

When an LLM context window fills up, most pipelines just truncate the tail and hope. That throws away closing brackets, rare identifiers and the one sentence that actually answered the question, while keeping a hundred copies of the word "the".

**Language:** Python | **Lines:** 90 | **Added:** 2026-04-06

## What this solves

This solves the context window overflow problem. Every long running LLM pipeline hits the same wall: the prompt is bigger than the budget, something has to go, and the eviction policy is almost always `tokens[:4096]` or `tokens[-4096:]`. Both are blunt. One kills the turn the user just typed, the other kills the schema definition. Neither knows that a rare product SKU carries more signal than a filler word, or that dropping an unmatched `}` breaks the parser downstream.

The failure mode in production is quiet and expensive. You do not get an exception. You get a model answering confidently about a document whose middle third silently vanished. You get JSON that fails to parse because the cut landed inside a brace pair, and your retry logic burns another full priced call. You get a chatbot that forgets a constraint stated four turns ago because the sliding window ate it in insertion order rather than importance order. Your users notice, your monitoring does not, because token count stayed inside the limit and the request returned HTTP 200.

Cost is the other half. Cutting 20 percent of a RAG prompt by dropping the least informative tokens instead of the last ones is a direct spend reduction that does not degrade the answer, provided you cut the right 20 percent. That is the entire job of this file. The trick it encodes: rare tokens carry more information than common ones, position in the sequence matters, and syntax matters more than filler. Score each token on all three, sort, then keep the top N.

## Why I built it

Tokenizer libraries count tokens. Vector stores rank chunks. Nothing in between ranks individual tokens inside a chunk you have already decided to include. The gap is at the eviction step, and everyone fills it with a slice expression written in thirty seconds and never revisited. Frameworks that offer "context compression" usually mean an extra LLM call to summarize, which costs money, adds latency and introduces a second place for hallucination.

I wanted the opposite: a scoring function with no model, no network call and no dependencies, cheap enough to run on every request, that you can read in one sitting and tune by changing three numbers.

## When to use it

- A RAG prompt assembled from retrieved chunks overflows the model budget and you need to shed tokens before the API call, not after the 400 response.
- A multi turn chatbot with a sliding window that keeps forgetting constraints stated early in the conversation.
- Long form document processing where you batch pages through a fixed budget and tail truncation keeps cutting mid structure.
- You are paying per token at volume and want to strip low information filler without an extra summarization call.
- You already have relevance scores from a reranker and want to fold them into a token level keep or drop decision.
- You need the eviction policy deterministic and auditable, so you can log exactly which indices were dropped.

## How it works

The core is `DynamicTokenPrioritizer`, constructed with `context_budget` and `decay_factor`. Two public methods do the work: `prioritize` returns every token with its score, and `retain` returns the indices you should keep.

Scoring is a weighted sum of three signals computed per token inside `prioritize`. First, `_calculate_entropy` returns `1.0 / (1.0 + count)` where count comes from `self.token_counts`. That is inverse frequency, not Shannon entropy, and it is the right shape for the job: a token seen once scores 0.5, a token seen ten times scores about 0.09. Second, `_position_weight` computes `1.0 - (index / total)` and takes the square root, so weight falls off gently across the head of the sequence and steeply near the tail. Note the direction carefully: index 0 gets weight 1.0 and the final index approaches 0, so whichever end of your list sits at index 0 is the end the scorer protects. Feed it newest first if you want recency to mean recency. Third, `semantic_scores` is an optional external relevance array, defaulting to 0.5 per token when you pass nothing.

Those three combine as `entropy * 0.3 + position * 0.3 + semantic * 0.4`, then get multiplied by a 1.5 structural boost when `_is_structural` fires. That predicate matches a fixed set of quotes, brackets and punctuation, and it also matches any token shorter than two characters, so single character tokens of any kind ride the boost. This is what keeps brace pairs and quote marks alive when the budget tightens, which is what saves your JSON parse.

Counting happens before scoring. `prioritize` walks the whole input incrementing `self.token_counts` first, so every instance of a repeated token shares the same frequency term and only position separates them. Those counts persist on the instance across calls, which means a token that was common in yesterday's batch is still penalized today. Deliberate state, and the main thing to watch.

Tokens are wrapped in a `Token` dataclass whose `__lt__` is inverted, returning `self.priority > other.priority`, which turns Python's min heap into a max heap by priority. The file calls `heapq.heapify` on the list, then produces its output through `sorted(key=lambda x: -x.priority)`, so the heapify is vestigial and the real cost is the O(n log n) sort.

`retain` layers a selection pass on top. It takes the top `count` token texts from the prioritized list, then walks the original sequence in order and keeps an index whenever its text is still in that pool, removing the match as it goes so duplicates behave as a multiset rather than a set. Because identical texts differ only by position weight and lower indices score higher, the earlier occurrence survives. The return is a sorted list of indices, so original order is preserved for reassembly.

## Usage

The file has no `__main__` block and no CLI. It is a class you import. Strip the trailing explanation block first, see Notes.

```python
from DynamicTokenPrioritizer import DynamicTokenPrioritizer

prioritizer = DynamicTokenPrioritizer(context_budget=4096, decay_factor=0.95)

tokens = ["The", "quick", "brown", "fox", "(", "jumps", "the", "the", ")", "lazy", "dog", "."]

# Score every token, sorted highest priority first
for text, score in prioritizer.prioritize(tokens)[:5]:
    print(f"{text!r:10} {score:.4f}")
# '('        0.8924
# ')'        0.7848
# '.'        0.6549
# 'The'      0.6500
# 'quick'    0.6372

# Keep the 5 most important tokens, get back original indices
keep = prioritizer.retain(tokens, 5)
print(keep)                          # [0, 1, 4, 8, 11]
print([tokens[i] for i in keep])     # ['The', 'quick', '(', ')', '.']

# Fold in external relevance from a reranker or embedding model
scores = [0.9, 0.2, 0.2, 0.8, 0.1, 0.3, 0.1, 0.1, 0.1, 0.2, 0.8, 0.1]
keep = prioritizer.retain(tokens, 5, semantic_scores=scores)
```

## Notes

- The file as committed does not compile. Lines 85 to 90 wrap the explanation in C style `/* */` delimiters, which is a `SyntaxError` in Python. Delete that block or convert it to `#` comments before importing.
- `context_budget` and `decay_factor` are stored on the instance and never read. Nothing in the file consumes them, and the `hashlib` import plus `self.hash_cache` are unused too. Pass whatever you like, then enforce the budget yourself through the `count` argument to `retain`.
- `token_counts` grows without bound and is never decayed or evicted, so scores are not reproducible across calls and a long lived process leaks slowly. `retain` calls `prioritize` internally, so running both on the same input double counts every token. Call one or the other, and construct a fresh instance per document if you want deterministic output.
- `_is_structural` treats any token under two characters as structural, so single letters, single digits and stray whitespace tokens all get the boost. If your tokenizer emits subword fragments this will overweight them.
- Selection inside `retain` matches by text using `list.remove`, a linear scan per match, so complexity is roughly O(n * k) for k retained tokens. `prioritize` returns `(text, priority)` pairs with no index, so use `retain` when you need to map back to positions.
- Requires Python 3.7 or later for `dataclasses`. No third party dependencies.
