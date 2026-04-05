use std::collections::BinaryHeap;
use std::cmp::Ordering;

#[derive(Clone)]
struct Embedding {
    id: String,
    vector: Vec<f32>,
    tokens: u16,
}

#[derive(Clone, Copy)]
struct ScoredResult {
    distance: f32,
    tokens: u16,
    idx: usize,
}

impl Ord for ScoredResult {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance.partial_cmp(&other.distance)
            .unwrap_or(Ordering::Equal)
            .reverse()
    }
}

impl PartialOrd for ScoredResult {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) }
}

impl Eq for ScoredResult {}

impl PartialEq for ScoredResult {
    fn eq(&self, other: &Self) -> bool {
        (self.distance - other.distance).abs() < 1e-6
    }
}

pub struct VectorSearchOptimizer {
    embeddings: Vec<Embedding>,
    dimension: usize,
    token_budget: u16,
}

impl VectorSearchOptimizer {
    pub fn new(dimension: usize, token_budget: u16) -> Self {
        Self { embeddings: Vec::new(), dimension, token_budget }
    }

    pub fn add(&mut self, id: String, vector: Vec<f32>, tokens: u16) {
        assert_eq!(vector.len(), self.dimension);
        self.embeddings.push(Embedding { id, vector, tokens });
    }

    pub fn search(&self, query: &[f32], k: usize) -> Vec<(String, f32, bool)> {
        assert_eq!(query.len(), self.dimension);

        let mut heap = BinaryHeap::with_capacity(k);
        let mut total_tokens = 0u16;

        for (idx, emb) in self.embeddings.iter().enumerate() {
            let dist = cosine_distance(query, &emb.vector);
            if heap.len() < k {
                heap.push(ScoredResult { distance: dist, tokens: emb.tokens, idx });
            } else if let Some(&worst) = heap.peek() {
                if dist < worst.distance {
                    heap.pop();
                    heap.push(ScoredResult { distance: dist, tokens: emb.tokens, idx });
                }
            }
        }

        let mut results = Vec::new();
        while let Some(scored) = heap.pop() {
            let emb = &self.embeddings[scored.idx];
            let within_budget = total_tokens + scored.tokens <= self.token_budget;
            if within_budget { total_tokens += scored.tokens; }
            results.push((emb.id.clone(), scored.distance, within_budget));
        }

        results.reverse();
        results
    }

    pub fn rerank(&self, ids: &[String], query: &[f32]) -> Vec<(String, f32)> {
        let mut scored: Vec<_> = ids.iter()
            .filter_map(|id| {
                self.embeddings.iter()
                    .find(|e| e.id == *id)
                    .map(|e| (id.clone(), cosine_distance(query, &e.vector)))
            })
            .collect();
        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        scored
    }
}

#[inline]
fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let mag_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let mag_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if mag_a == 0.0 || mag_b == 0.0 { 1.0 } else { 1.0 - (dot / (mag_a * mag_b)) }
}
