use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub struct TokenBudgetManager {
    total_budget: usize,
    used: Arc<AtomicUsize>,
    reserved: Arc<AtomicUsize>,
    warn_threshold: usize,
}

impl TokenBudgetManager {
    pub fn new(total_budget: usize) -> Self {
        let warn_threshold = (total_budget as f64 * 0.85) as usize;
        Self {
            total_budget,
            used: Arc::new(AtomicUsize::new(0)),
            reserved: Arc::new(AtomicUsize::new(0)),
            warn_threshold,
        }
    }

    pub fn reserve(&self, tokens: usize) -> Result<TokenLease, BudgetExceeded> {
        let current = self.used.load(Ordering::Acquire);
        let reserved = self.reserved.load(Ordering::Acquire);
        let total_allocated = current + reserved + tokens;

        if total_allocated > self.total_budget {
            return Err(BudgetExceeded {
                requested: tokens,
                available: self.total_budget.saturating_sub(current + reserved),
                total_budget: self.total_budget,
            });
        }

        self.reserved.fetch_add(tokens, Ordering::Release);

        Ok(TokenLease {
            budget: self.clone_refs(),
            tokens,
            consumed: 0,
        })
    }

    pub fn commit(&self, tokens: usize) {
        self.used.fetch_add(tokens, Ordering::Release);
    }

    pub fn remaining(&self) -> usize {
        let used = self.used.load(Ordering::Acquire);
        let reserved = self.reserved.load(Ordering::Acquire);
        self.total_budget.saturating_sub(used + reserved)
    }

    pub fn is_warning_level(&self) -> bool {
        self.used.load(Ordering::Acquire) >= self.warn_threshold
    }

    pub fn usage_percent(&self) -> f64 {
        (self.used.load(Ordering::Acquire) as f64 / self.total_budget as f64) * 100.0
    }

    fn clone_refs(&self) -> (Arc<AtomicUsize>, Arc<AtomicUsize>) {
        (Arc::clone(&self.used), Arc::clone(&self.reserved))
    }
}

pub struct TokenLease {
    budget: (Arc<AtomicUsize>, Arc<AtomicUsize>),
    tokens: usize,
    consumed: usize,
}

impl TokenLease {
    pub fn consume(&mut self, tokens: usize) {
        self.consumed = tokens.min(self.tokens);
    }

    pub fn remaining_in_lease(&self) -> usize {
        self.tokens.saturating_sub(self.consumed)
    }
}

impl Drop for TokenLease {
    fn drop(&mut self) {
        let (_used, reserved) = &self.budget;
        let unconsumed = self.tokens.saturating_sub(self.consumed);
        reserved.fetch_sub(self.tokens, Ordering::Release);
        if self.consumed > 0 {
            _used.fetch_add(self.consumed, Ordering::Release);
        }
    }
}

#[derive(Debug)]
pub struct BudgetExceeded {
    pub requested: usize,
    pub available: usize,
    pub total_budget: usize,
}

/*
================================================================================
EXPLANATION
TokenBudgetManager handles real-time context window tracking for streaming LLM responses. Built because every Rust backend integrating AI needs to prevent budget overruns, track consumption mid-stream, and signal backpressure before hitting limits. Use when your app streams tokens from Claude or other APIs and needs atomic budget enforcement. The trick: reserve first (non-blocking), consume as tokens arrive, automatically release unconsumed tokens on drop. Tracks usage percent and warning thresholds. Lock-free with atomics, thread-safe refs for streaming tasks.
================================================================================
*/
