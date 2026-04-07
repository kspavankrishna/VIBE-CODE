package main

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type TokenRequest struct {
	ID           string
	TokensNeeded int
	Fn           func(ctx context.Context) error
	ResultChan   chan error
}

type ConcurrentTokenWindow struct {
	maxTokens      int64
	tokensPerSec   int64
	currentTokens  int64
	windowStart    time.Time
	maxConcurrent  int
	sem            chan struct{}
	queue          chan *TokenRequest
	mu             sync.RWMutex
	running        bool
	circuitOpen    bool
	failureCount   int32
	failureWindow  time.Time
}

func NewConcurrentTokenWindow(maxTokens int64, tokensPerSec int64, maxConcurrent int) *ConcurrentTokenWindow {
	ctw := &ConcurrentTokenWindow{
		maxTokens:     maxTokens,
		tokensPerSec:  tokensPerSec,
		currentTokens: maxTokens,
		windowStart:   time.Now(),
		maxConcurrent: maxConcurrent,
		sem:           make(chan struct{}, maxConcurrent),
		queue:         make(chan *TokenRequest, maxConcurrent*2),
		running:       true,
	}
	go ctw.refillTokens()
	go ctw.processQueue()
	go ctw.monitorCircuitBreaker()
	return ctw
}

func (ctw *ConcurrentTokenWindow) refillTokens() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		ctw.mu.Lock()
		elapsed := time.Since(ctw.windowStart).Seconds()
		refill := int64(float64(ctw.tokensPerSec) * elapsed)
		ctw.currentTokens = min(ctw.maxTokens, refill)
		ctw.mu.Unlock()
	}
}

func (ctw *ConcurrentTokenWindow) processQueue() {
	for req := range ctw.queue {
		ctw.mu.RLock()
		if ctw.circuitOpen {
			ctw.mu.RUnlock()
			req.ResultChan <- ErrCircuitOpen
			continue
		}
		if ctw.currentTokens < int64(req.TokensNeeded) {
			ctw.mu.RUnlock()
			ctw.queue <- req
			time.Sleep(10 * time.Millisecond)
			continue
		}
		ctw.currentTokens -= int64(req.TokensNeeded)
		ctw.mu.RUnlock()

		ctw.sem <- struct{}{}
		go func(r *TokenRequest) {
			defer func() { <-ctw.sem }()
			err := r.Fn(context.Background())
			if err != nil {
				atomic.AddInt32(&ctw.failureCount, 1)
			}
			r.ResultChan <- err
		}(req)
	}
}

func (ctw *ConcurrentTokenWindow) monitorCircuitBreaker() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		failures := atomic.LoadInt32(&ctw.failureCount)
		if failures > 5 {
			ctw.mu.Lock()
			ctw.circuitOpen = true
			ctw.mu.Unlock()
			time.Sleep(2 * time.Second)
			atomic.StoreInt32(&ctw.failureCount, 0)
			ctw.mu.Lock()
			ctw.circuitOpen = false
			ctw.mu.Unlock()
		}
	}
}

func (ctw *ConcurrentTokenWindow) Submit(req *TokenRequest) error {
	ctw.mu.RLock()
	if !ctw.running {
		ctw.mu.RUnlock()
		return ErrClosed
	}
	ctw.mu.RUnlock()
	select {
	case ctw.queue <- req:
		return nil
	default:
		return ErrQueueFull
	}
}

func (ctw *ConcurrentTokenWindow) Close() {
	ctw.mu.Lock()
	ctw.running = false
	ctw.mu.Unlock()
	close(ctw.queue)
}

func (ctw *ConcurrentTokenWindow) GetAvailableTokens() int64 {
	ctw.mu.RLock()
	defer ctw.mu.RUnlock()
	return ctw.currentTokens
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

var (
	ErrCircuitOpen = &TokenError{"circuit breaker open"}
	ErrClosed      = &TokenError{"token window closed"}
	ErrQueueFull   = &TokenError{"queue full"}
)

type TokenError struct{ msg string }

func (e *TokenError) Error() string { return e.msg }

/*
================================================================================
EXPLANATION
ConcurrentTokenWindow manages goroutines for LLM APIs with token budgets and backpressure. Built because token-limited APIs need smart concurrency — you can't just fire goroutines, you need to respect token windows, queue overflow, and automatic circuit breaking when failures spike. Use this for batch processing against GPT/Claude APIs with concurrent rate limiting. The trick: refill tokens at a steady rate, queue requests that exceed budget, monitor failures and open the circuit if error rate spikes. This prevents cascading failures and keeps throughput constant. Drop into any service that calls token-limited APIs and needs predictable backpressure and failure recovery.
================================================================================
*/
