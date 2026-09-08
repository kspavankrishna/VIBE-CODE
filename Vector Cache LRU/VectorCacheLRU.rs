use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub struct VectorCacheLRU {
    cache: Arc<Mutex<HashMap<String, (Vec<f32>, usize)>>>,
    access_order: Arc<Mutex<Vec<String>>>,
    max_entries: usize,
}

impl VectorCacheLRU {
    pub fn new(max_entries: usize) -> Self {
        Self {
            cache: Arc::new(Mutex::new(HashMap::new())),
            access_order: Arc::new(Mutex::new(Vec::new())),
            max_entries,
        }
    }

    pub fn get(&self, key: &str) -> Option<Vec<f32>> {
        let mut cache = self.cache.lock().unwrap();
        let mut order = self.access_order.lock().unwrap();

        if let Some((vector, _)) = cache.get(key) {
            order.retain(|k| k != key);
            order.push(key.to_string());
            Some(vector.clone())
        } else {
            None
        }
    }

    pub fn put(&self, key: String, vector: Vec<f32>) {
        let mut cache = self.cache.lock().unwrap();
        let mut order = self.access_order.lock().unwrap();

        order.retain(|k| k != &key);
        order.push(key.clone());

        cache.insert(key, (vector, order.len()));

        while cache.len() > self.max_entries && !order.is_empty() {
            if let Some(oldest) = order.first().cloned() {
                order.remove(0);
                cache.remove(&oldest);
            }
        }
    }

    pub fn size(&self) -> usize {
        self.cache.lock().unwrap().len()
    }

    pub fn memory_usage(&self) -> usize {
        let cache = self.cache.lock().unwrap();
        cache.values().map(|(v, _)| v.len() * 4).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_eviction() {
        let cache = VectorCacheLRU::new(2);
        cache.put("a".to_string(), vec![1.0, 2.0]);
        cache.put("b".to_string(), vec![3.0, 4.0]);
        cache.put("c".to_string(), vec![5.0, 6.0]);
        
        assert_eq!(cache.size(), 2);
        assert!(cache.get("a").is_none());
    }

    #[test]
    fn test_access_updates_order() {
        let cache = VectorCacheLRU::new(2);
        cache.put("a".to_string(), vec![1.0]);
        cache.put("b".to_string(), vec![2.0]);
        cache.get("a");
        cache.put("c".to_string(), vec![3.0]);
        
        assert!(cache.get("a").is_some());
        assert!(cache.get("b").is_none());
    }
}

/*
================================================================================
EXPLANATION
VectorCacheLRU manages embedding vectors with automatic eviction when memory limits are hit. Built because production AI systems need to cache embeddings without exploding memory costs—especially when running inference at scale on constrained hardware. Use it in RAG pipelines, vector search backends, or any system ingesting embeddings from APIs. The trick: maintains access order and evicts the least recently used vector when capacity is reached, keeping hot vectors in cache and cold ones out. Thread-safe via Arc<Mutex>, so you can share across async tasks. Drop this into your embedding cache layer to stop recomputing expensive vectors.
================================================================================
*/
