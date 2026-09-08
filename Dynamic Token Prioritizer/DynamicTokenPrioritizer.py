import heapq
from dataclasses import dataclass
from typing import List, Tuple
import hashlib

@dataclass
class Token:
    text: str
    index: int
    priority: float = 0.0
    
    def __lt__(self, other):
        return self.priority > other.priority

class DynamicTokenPrioritizer:
    def __init__(self, context_budget: int = 4096, decay_factor: float = 0.95):
        self.context_budget = context_budget
        self.decay_factor = decay_factor
        self.token_counts = {}
        self.hash_cache = {}
    
    def _calculate_entropy(self, token: str) -> float:
        """Rare tokens (low frequency) have higher priority—they carry more information."""
        count = self.token_counts.get(token, 1)
        return 1.0 / (1.0 + count)
    
    def _position_weight(self, index: int, total: int) -> float:
        """Recent tokens decay logarithmically; very old tokens become negligible."""
        if total == 0:
            return 1.0
        recency = 1.0 - (index / total)
        return recency ** 0.5
    
    def _is_structural(self, token: str) -> bool:
        """Structural tokens (quotes, brackets, punctuation) are essentials."""
        structural = {'"', "'", '(', ')', '[', ']', '{', '}', ':', ',', '.', '!', '?'}
        return token in structural or len(token) < 2
    
    def prioritize(self, tokens: List[str], semantic_scores: List[float] = None) -> List[Tuple[str, float]]:
        """
        Assign priority scores to tokens.
        semantic_scores: optional external relevance scores (0-1).
        Returns: [(token, priority), ...] sorted by priority descending.
        """
        if not tokens:
            return []
        
        for token in tokens:
            self.token_counts[token] = self.token_counts.get(token, 0) + 1
        
        token_objects = []
        total_tokens = len(tokens)
        
        for index, token in enumerate(tokens):
            entropy = self._calculate_entropy(token)
            position = self._position_weight(index, total_tokens)
            
            is_structural = self._is_structural(token)
            structural_boost = 1.5 if is_structural else 1.0
            
            semantic = semantic_scores[index] if semantic_scores and index < len(semantic_scores) else 0.5
            
            priority = (entropy * 0.3 + position * 0.3 + semantic * 0.4) * structural_boost
            token_objects.append(Token(text=token, index=index, priority=priority))
        
        heapq.heapify(token_objects)
        return [(t.text, t.priority) for t in sorted(token_objects, key=lambda x: -x.priority)]
    
    def retain(self, tokens: List[str], count: int, semantic_scores: List[float] = None) -> List[int]:
        """
        Returns indices of the top `count` most important tokens to keep.
        Use when context window is full—drop low-priority tokens first.
        """
        prioritized = self.prioritize(tokens, semantic_scores)
        top_tokens = [t[0] for t in prioritized[:count]]
        
        kept_indices = []
        for i, token in enumerate(tokens):
            if len(kept_indices) < count and token in top_tokens:
                kept_indices.append(i)
                top_tokens.remove(token)
        
        return sorted(kept_indices)

/*
================================================================================
EXPLANATION
This solves the context window overflow problem. When LLM context gets full, you need to drop tokens intelligently—not just truncate tail. DynamicTokenPrioritizer ranks tokens by three factors: how rare they are (entropy), how recent (position), and their semantic importance. Structural tokens (quotes, brackets) always get boosted because you need them for parsing. Use it when building RAG systems, multi-turn chatbots, or long-form document processing where context budget is tight. The trick: rare tokens carry more information than common ones, recent context matters more than old, and syntax matters more than filler. Drop this into any LLM pipeline where you're batching documents and need smart token eviction policies instead of dumb truncation.
================================================================================
*/
