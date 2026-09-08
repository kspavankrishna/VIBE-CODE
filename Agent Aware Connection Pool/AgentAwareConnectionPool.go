package main

// Package-level component: AgentAwareConnectionPool.
//
// A database/sql admission-control layer purpose-built for the era of
// concurrent AI agents hammering a shared Postgres/MySQL/etc. database.
// Ordinary connection pools (database/sql's own MaxOpenConns, pgbouncer,
// etc.) treat every caller identically: whichever goroutine calls first
// gets served first, and a single misbehaving agent (a runaway retry loop,
// a badly-batched tool call, a fan-out over 500 rows) can starve every
// other agent sharing the pool. This file adds two things that plain
// pools do not have:
//
//  1. Weighted Fair Queuing (WFQ) admission, borrowed from network packet
//     scheduling, so connection slots are handed out in virtual-time order
//     per logical agent/tenant rather than first-come-first-served. A
//     misbehaving agent that floods the pool with requests only crowds
//     out its own future requests, not everyone else's.
//  2. An AIMD (additive-increase / multiplicative-decrease) feedback loop
//     that watches real service latency and grows or shrinks the live
//     MaxOpenConns budget automatically, the same congestion-control idea
//     TCP uses, so the pool self-tunes instead of needing a hand-picked
//     pool size that's either too small (queueing) or too large (the DB
//     falls over under agent bursts).
//
// It is a single dependency-free file: only the Go standard library.

import (
	"container/heap"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

var (
	// ErrPoolClosed is returned by Acquire/QueryContext/ExecContext once
	// Close has been called, and to any waiter still queued at shutdown.
	ErrPoolClosed = errors.New("agentpool: pool is closed")

	// ErrAgentCircuitOpen is returned when the calling agent's own
	// circuit breaker is open because its recent requests kept failing.
	// Other agents are unaffected: the breaker is per-agent, not global.
	ErrAgentCircuitOpen = errors.New("agentpool: agent circuit breaker is open")

	// ErrEmptyAgentID is returned when Acquire is called with an empty
	// agent identifier, since fairness has no meaning without one.
	ErrEmptyAgentID = errors.New("agentpool: agentID must not be empty")
)

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

// Config controls pool sizing bounds and the AIMD/EWMA/breaker tuning.
type Config struct {
	// MinOpen is the floor the AIMD controller will never shrink below,
	// and the value the pool starts at.
	MinOpen int
	// MaxOpenCap is the ceiling the AIMD controller will never grow past,
	// regardless of how much headroom it observes.
	MaxOpenCap int
	// TargetLatency is the service-latency budget for a single admitted
	// query. Sustained latency above this shrinks the pool; latency
	// comfortably below it, combined with an actual queue, grows it.
	TargetLatency time.Duration
	// ControlInterval is how often the AIMD loop re-evaluates.
	ControlInterval time.Duration
	// EWMAAlpha is the smoothing factor (0,1] for the latency estimate;
	// higher reacts faster, lower is steadier under noise.
	EWMAAlpha float64
	// AIMDIncrease is the additive step used when growing MaxOpenConns.
	AIMDIncrease int
	// AIMDDecreaseFactor is the multiplicative shrink factor (0,1)
	// applied to MaxOpenConns when latency exceeds TargetLatency.
	AIMDDecreaseFactor float64
	// DefaultWeight is used when a caller passes weight <= 0 to Acquire.
	DefaultWeight int
	// BreakerFailureThreshold is the number of consecutive failed
	// queries from one agent before that agent's breaker opens.
	BreakerFailureThreshold int32
	// BreakerCooldown is how long an open breaker stays open before the
	// agent is allowed to try again.
	BreakerCooldown time.Duration
}

// DefaultConfig returns sane defaults for a small-to-mid size OLTP
// database being shared by several concurrent AI agents.
func DefaultConfig() Config {
	return Config{
		MinOpen:                 4,
		MaxOpenCap:              64,
		TargetLatency:           120 * time.Millisecond,
		ControlInterval:         2 * time.Second,
		EWMAAlpha:               0.3,
		AIMDIncrease:            2,
		AIMDDecreaseFactor:      0.7,
		DefaultWeight:           1,
		BreakerFailureThreshold: 5,
		BreakerCooldown:         10 * time.Second,
	}
}

func (c Config) normalized() Config {
	if c.MinOpen <= 0 {
		c.MinOpen = 1
	}
	if c.MaxOpenCap < c.MinOpen {
		c.MaxOpenCap = c.MinOpen
	}
	if c.TargetLatency <= 0 {
		c.TargetLatency = 100 * time.Millisecond
	}
	if c.ControlInterval <= 0 {
		c.ControlInterval = 2 * time.Second
	}
	if c.EWMAAlpha <= 0 || c.EWMAAlpha > 1 {
		c.EWMAAlpha = 0.3
	}
	if c.AIMDIncrease <= 0 {
		c.AIMDIncrease = 1
	}
	if c.AIMDDecreaseFactor <= 0 || c.AIMDDecreaseFactor >= 1 {
		c.AIMDDecreaseFactor = 0.7
	}
	if c.DefaultWeight <= 0 {
		c.DefaultWeight = 1
	}
	if c.BreakerFailureThreshold <= 0 {
		c.BreakerFailureThreshold = 5
	}
	if c.BreakerCooldown <= 0 {
		c.BreakerCooldown = 10 * time.Second
	}
	return c
}

// ---------------------------------------------------------------------------
// Waiter queue: a virtual-time min-heap (Weighted Fair Queuing)
// ---------------------------------------------------------------------------

type waiter struct {
	agentID  string
	weight   int
	vFinish  float64
	seq      uint64 // tie-breaker so equal vFinish stays FIFO and stable
	ready    chan struct{}
	admitted bool // valid only after ready is closed
	index    int  // maintained by heap.Interface for O(log n) removal
}

type waiterHeap []*waiter

func (h waiterHeap) Len() int { return len(h) }
func (h waiterHeap) Less(i, j int) bool {
	if h[i].vFinish != h[j].vFinish {
		return h[i].vFinish < h[j].vFinish
	}
	return h[i].seq < h[j].seq
}
func (h waiterHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *waiterHeap) Push(x any) {
	w := x.(*waiter)
	w.index = len(*h)
	*h = append(*h, w)
}
func (h *waiterHeap) Pop() any {
	old := *h
	n := len(old)
	w := old[n-1]
	old[n-1] = nil
	w.index = -1
	*h = old[:n-1]
	return w
}

// ---------------------------------------------------------------------------
// Per-agent state
// ---------------------------------------------------------------------------

type agentState struct {
	active           int
	queued           int
	lastVFinish      float64
	consecutiveFail  int32
	breakerOpenUntil time.Time
}

// ---------------------------------------------------------------------------
// Lease
// ---------------------------------------------------------------------------

// Lease represents one admitted connection-slot. It must be released
// exactly once; Release is idempotent-safe against double calls.
type Lease struct {
	pool       *AgentPool
	agentID    string
	weight     int
	acquiredAt time.Time
	failed     int32 // 0/1, set via MarkFailed
	released   int32 // 0/1 guard
}

// MarkFailed flags the work done under this lease as failed, which is
// counted toward the issuing agent's circuit breaker. Call it before
// Release when the query/exec you ran returned an error worth tracking
// (a syntax error from bad caller input should generally not be marked;
// a timeout, a connection reset, or a DB-side error should).
func (l *Lease) MarkFailed() {
	atomic.StoreInt32(&l.failed, 1)
}

// AgentID returns the agent identifier this lease was admitted under.
func (l *Lease) AgentID() string { return l.agentID }

// Weight returns the priority weight this lease was admitted under.
func (l *Lease) Weight() int { return l.weight }

// Release returns the slot to the pool and feeds latency/failure signal
// back into the AIMD controller and the agent's breaker. Safe to call
// more than once; only the first call has an effect.
func (l *Lease) Release() {
	if !atomic.CompareAndSwapInt32(&l.released, 0, 1) {
		return
	}
	l.pool.release(l)
}

// ---------------------------------------------------------------------------
// AgentPool
// ---------------------------------------------------------------------------

// AgentPool wraps a *sql.DB with fairness-aware, self-tuning admission
// control across many concurrently-calling AI agents / tenants / callers.
type AgentPool struct {
	db  *sql.DB
	cfg Config

	mu           sync.Mutex
	heap         waiterHeap
	agents       map[string]*agentState
	globalVClock float64
	activeSlots  int
	seq          uint64
	closed       bool
	latencyEWMA  float64 // seconds

	maxSlots int32 // atomic; mirrors db.SetMaxOpenConns

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewAgentPool wraps db and starts the background AIMD control loop.
// The caller retains ownership of db and is responsible for closing it;
// AgentPool.Close only stops the controller and fails queued waiters.
func NewAgentPool(db *sql.DB, cfg Config) *AgentPool {
	cfg = cfg.normalized()
	p := &AgentPool{
		db:       db,
		cfg:      cfg,
		agents:   make(map[string]*agentState),
		maxSlots: int32(cfg.MinOpen),
		stopCh:   make(chan struct{}),
	}
	db.SetMaxOpenConns(cfg.MinOpen)
	p.wg.Add(1)
	go p.controlLoop()
	return p
}

// Acquire blocks (respecting ctx) until a connection-slot is admitted for
// agentID, or returns an error. weight controls the agent's share of the
// pool relative to other agents currently contending for it: a weight of
// 2 gets roughly twice the admission priority of a weight of 1 under
// contention. Pass 0 to use Config.DefaultWeight.
func (p *AgentPool) Acquire(ctx context.Context, agentID string, weight int) (*Lease, error) {
	if agentID == "" {
		return nil, ErrEmptyAgentID
	}
	if weight <= 0 {
		weight = p.cfg.DefaultWeight
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrPoolClosed
	}
	st := p.agentOrCreateLocked(agentID)
	if now := time.Now(); now.Before(st.breakerOpenUntil) {
		p.mu.Unlock()
		return nil, ErrAgentCircuitOpen
	}

	const cost = 1.0
	vStart := math.Max(p.globalVClock, st.lastVFinish)
	vFinish := vStart + cost/float64(weight)
	st.lastVFinish = vFinish
	st.queued++

	w := &waiter{
		agentID: agentID,
		weight:  weight,
		vFinish: vFinish,
		seq:     p.seq,
		ready:   make(chan struct{}),
	}
	p.seq++
	heap.Push(&p.heap, w)
	p.promoteLocked()
	p.mu.Unlock()

	select {
	case <-w.ready:
		return p.finishAdmission(w)
	case <-ctx.Done():
		p.mu.Lock()
		if w.index >= 0 {
			heap.Remove(&p.heap, w.index)
			st := p.agents[agentID]
			if st != nil {
				st.queued--
			}
			p.mu.Unlock()
			return nil, ctx.Err()
		}
		p.mu.Unlock()
		// Lost the race: promotion already happened concurrently.
		// Drain the admission and immediately hand the slot back so
		// it isn't leaked, then report the cancellation to the caller.
		<-w.ready
		lease, err := p.finishAdmission(w)
		if err == nil {
			lease.Release()
		}
		return nil, ctx.Err()
	}
}

// finishAdmission converts an admitted waiter into a Lease. w.ready is
// guaranteed closed by the time this is called; the happens-before edge
// from the channel close makes reading w.admitted here safe without a
// lock.
func (p *AgentPool) finishAdmission(w *waiter) (*Lease, error) {
	if !w.admitted {
		return nil, ErrPoolClosed
	}
	return &Lease{
		pool:       p,
		agentID:    w.agentID,
		weight:     w.weight,
		acquiredAt: time.Now(),
	}, nil
}

// promoteLocked admits as many queued waiters, in virtual-finish order,
// as current capacity allows. Caller must hold p.mu.
func (p *AgentPool) promoteLocked() {
	max := int(atomic.LoadInt32(&p.maxSlots))
	for p.activeSlots < max && p.heap.Len() > 0 {
		w := heap.Pop(&p.heap).(*waiter)
		p.activeSlots++
		if st := p.agents[w.agentID]; st != nil {
			st.active++
			st.queued--
		}
		p.globalVClock = w.vFinish
		w.admitted = true
		close(w.ready)
	}
	if p.heap.Len() == 0 {
		// Renormalize virtual time so long-lived pools don't
		// accumulate unbounded float64 drift across idle periods.
		p.globalVClock = 0
		for _, st := range p.agents {
			st.lastVFinish = 0
		}
	}
}

func (p *AgentPool) agentOrCreateLocked(agentID string) *agentState {
	st, ok := p.agents[agentID]
	if !ok {
		st = &agentState{}
		p.agents[agentID] = st
	}
	return st
}

// release is invoked by Lease.Release. It frees the slot, promotes the
// next waiter(s), and feeds latency/failure back into the controller.
func (p *AgentPool) release(l *Lease) {
	elapsed := time.Since(l.acquiredAt)
	failed := atomic.LoadInt32(&l.failed) == 1

	p.mu.Lock()
	p.activeSlots--
	if st := p.agents[l.agentID]; st != nil {
		st.active--
		if failed {
			st.consecutiveFail++
			if st.consecutiveFail >= p.cfg.BreakerFailureThreshold {
				st.breakerOpenUntil = time.Now().Add(p.cfg.BreakerCooldown)
				st.consecutiveFail = 0
			}
		} else {
			st.consecutiveFail = 0
		}
	}
	sec := elapsed.Seconds()
	if p.latencyEWMA == 0 {
		p.latencyEWMA = sec
	} else {
		p.latencyEWMA = p.cfg.EWMAAlpha*sec + (1-p.cfg.EWMAAlpha)*p.latencyEWMA
	}
	p.promoteLocked()
	p.mu.Unlock()
}

// ---------------------------------------------------------------------------
// AIMD control loop
// ---------------------------------------------------------------------------

func (p *AgentPool) controlLoop() {
	defer p.wg.Done()
	ticker := time.NewTicker(p.cfg.ControlInterval)
	defer ticker.Stop()
	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.adjust()
		}
	}
}

func (p *AgentPool) adjust() {
	p.mu.Lock()
	ewma := p.latencyEWMA
	queueDepth := p.heap.Len()
	cur := atomic.LoadInt32(&p.maxSlots)
	target := cur

	switch {
	case ewma > 0 && time.Duration(ewma*float64(time.Second)) > p.cfg.TargetLatency:
		// Overloaded: back off multiplicatively, like TCP on a lost
		// packet. This is the single most important safety property
		// of the whole controller: growth is gentle, retreat is fast.
		shrunk := float64(cur) * p.cfg.AIMDDecreaseFactor
		target = int32(math.Max(float64(p.cfg.MinOpen), math.Floor(shrunk)))
	case queueDepth > 0 &&
		(ewma == 0 || time.Duration(ewma*float64(time.Second)) < time.Duration(float64(p.cfg.TargetLatency)*0.8)):
		// There is real demand queued and latency has headroom:
		// grow additively.
		grown := cur + int32(p.cfg.AIMDIncrease)
		if grown > int32(p.cfg.MaxOpenCap) {
			grown = int32(p.cfg.MaxOpenCap)
		}
		target = grown
	}

	if target != cur {
		atomic.StoreInt32(&p.maxSlots, target)
		p.db.SetMaxOpenConns(int(target))
		p.promoteLocked()
	}
	p.mu.Unlock()
}

// ---------------------------------------------------------------------------
// Convenience query wrappers
// ---------------------------------------------------------------------------

// QueryContext acquires a fair-share slot for agentID, runs the query,
// and releases the slot. The admission slot is held only for the
// duration of issuing the query, not for the lifetime of the returned
// *sql.Rows — callers should still Close() the rows as usual.
func (p *AgentPool) QueryContext(ctx context.Context, agentID string, weight int, query string, args ...any) (*sql.Rows, error) {
	lease, err := p.Acquire(ctx, agentID, weight)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	rows, err := p.db.QueryContext(ctx, query, args...)
	if err != nil {
		lease.MarkFailed()
		return nil, err
	}
	return rows, nil
}

// QueryRowContext is the QueryRow analogue of QueryContext.
func (p *AgentPool) QueryRowContext(ctx context.Context, agentID string, weight int, query string, args ...any) (*sql.Row, error) {
	lease, err := p.Acquire(ctx, agentID, weight)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	return p.db.QueryRowContext(ctx, query, args...), nil
}

// ExecContext acquires a fair-share slot for agentID, runs the statement,
// and releases the slot before returning.
func (p *AgentPool) ExecContext(ctx context.Context, agentID string, weight int, query string, args ...any) (sql.Result, error) {
	lease, err := p.Acquire(ctx, agentID, weight)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	res, err := p.db.ExecContext(ctx, query, args...)
	if err != nil {
		lease.MarkFailed()
		return nil, err
	}
	return res, nil
}

// ---------------------------------------------------------------------------
// Observability
// ---------------------------------------------------------------------------

// AgentStats is a point-in-time snapshot for a single agent.
type AgentStats struct {
	Active       int
	Queued       int
	BreakerOpen  bool
	CooldownLeft time.Duration
}

// Stats is a point-in-time snapshot of the whole pool.
type Stats struct {
	MaxOpen     int
	ActiveSlots int
	QueueDepth  int
	LatencyEWMA time.Duration
	Agents      map[string]AgentStats
}

// Snapshot returns a consistent point-in-time view of the pool's state,
// suitable for logging, dashboards, or feeding an alerting rule.
func (p *AgentPool) Snapshot() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	agents := make(map[string]AgentStats, len(p.agents))
	for id, st := range p.agents {
		open := now.Before(st.breakerOpenUntil)
		var left time.Duration
		if open {
			left = st.breakerOpenUntil.Sub(now)
		}
		agents[id] = AgentStats{
			Active:       st.active,
			Queued:       st.queued,
			BreakerOpen:  open,
			CooldownLeft: left,
		}
	}
	return Stats{
		MaxOpen:     int(atomic.LoadInt32(&p.maxSlots)),
		ActiveSlots: p.activeSlots,
		QueueDepth:  p.heap.Len(),
		LatencyEWMA: time.Duration(p.latencyEWMA * float64(time.Second)),
		Agents:      agents,
	}
}

// PrometheusText renders the current snapshot in Prometheus text
// exposition format, with no dependency on any Prometheus client
// library. Wire it into an http.HandlerFunc for /metrics as-is.
func (p *AgentPool) PrometheusText() string {
	s := p.Snapshot()
	var b strings.Builder

	fmt.Fprintf(&b, "# HELP agentpool_max_open_connections Current adaptive MaxOpenConns budget.\n")
	fmt.Fprintf(&b, "# TYPE agentpool_max_open_connections gauge\n")
	fmt.Fprintf(&b, "agentpool_max_open_connections %d\n", s.MaxOpen)

	fmt.Fprintf(&b, "# HELP agentpool_active_connections Slots currently admitted.\n")
	fmt.Fprintf(&b, "# TYPE agentpool_active_connections gauge\n")
	fmt.Fprintf(&b, "agentpool_active_connections %d\n", s.ActiveSlots)

	fmt.Fprintf(&b, "# HELP agentpool_queue_depth Waiters queued across all agents.\n")
	fmt.Fprintf(&b, "# TYPE agentpool_queue_depth gauge\n")
	fmt.Fprintf(&b, "agentpool_queue_depth %d\n", s.QueueDepth)

	fmt.Fprintf(&b, "# HELP agentpool_latency_ewma_seconds Smoothed admitted-query latency.\n")
	fmt.Fprintf(&b, "# TYPE agentpool_latency_ewma_seconds gauge\n")
	fmt.Fprintf(&b, "agentpool_latency_ewma_seconds %f\n", s.LatencyEWMA.Seconds())

	ids := make([]string, 0, len(s.Agents))
	for id := range s.Agents {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	fmt.Fprintf(&b, "# HELP agentpool_agent_active Active slots held by one agent.\n")
	fmt.Fprintf(&b, "# TYPE agentpool_agent_active gauge\n")
	for _, id := range ids {
		fmt.Fprintf(&b, "agentpool_agent_active{agent=%q} %d\n", id, s.Agents[id].Active)
	}

	fmt.Fprintf(&b, "# HELP agentpool_agent_queue_depth Waiters queued for one agent.\n")
	fmt.Fprintf(&b, "# TYPE agentpool_agent_queue_depth gauge\n")
	for _, id := range ids {
		fmt.Fprintf(&b, "agentpool_agent_queue_depth{agent=%q} %d\n", id, s.Agents[id].Queued)
	}

	fmt.Fprintf(&b, "# HELP agentpool_agent_breaker_open Whether an agent's circuit breaker is open.\n")
	fmt.Fprintf(&b, "# TYPE agentpool_agent_breaker_open gauge\n")
	for _, id := range ids {
		v := 0
		if s.Agents[id].BreakerOpen {
			v = 1
		}
		fmt.Fprintf(&b, "agentpool_agent_breaker_open{agent=%q} %d\n", id, v)
	}

	return b.String()
}

// ---------------------------------------------------------------------------
// Shutdown
// ---------------------------------------------------------------------------

// Close stops the AIMD control loop and fails every currently-queued
// waiter with ErrPoolClosed. It does not close the underlying *sql.DB;
// the caller owns that lifecycle. Close is idempotent.
func (p *AgentPool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	for p.heap.Len() > 0 {
		w := heap.Pop(&p.heap).(*waiter)
		// admitted stays false: finishAdmission will report
		// ErrPoolClosed to whichever goroutine is waiting on this.
		close(w.ready)
	}
	p.mu.Unlock()

	close(p.stopCh)
	p.wg.Wait()
	return nil
}

/*
================================================================================
EXPLANATION

This solves the connection-storm problem that shows up the moment you put
more than one AI agent in front of the same database. Once you have several
agents (or agent instances, or parallel tool calls inside one agent) hitting
Postgres or MySQL at once, a plain database/sql pool with a fixed
MaxOpenConns either queues everyone first-come-first-served, which lets one
noisy agent starve the rest, or you leave it uncapped, which lets a burst
take the database down. Neither option is good enough once agents are
calling your database on their own schedule instead of a human clicking a
button.

Built because I kept seeing the same failure shape in agent-heavy backends:
one workflow fans out fifty queries, the pool empties out, every other
agent's request queues behind it, latency spikes everywhere at once, and
nobody can tell which caller actually caused it. Fixed pool sizes and plain
semaphores don't have any concept of "which agent" or "how much load can the
database actually take right now," so they can't fix either half of that.

Use it when you're running two or more independent AI agents, agent
sessions, or tenants against one shared SQL database and you need both
fairness between them and a pool size that reacts to real load instead of a
number you picked once and never revisited. It's a drop-in wrapper around
any existing *sql.DB, so it works with Postgres, MySQL, SQLite, or anything
else database/sql supports.

The trick is combining two ideas from outside typical backend code. First,
admission uses Weighted Fair Queuing, the same virtual-time scheduling
algorithm routers use to share bandwidth between network flows: every
request gets a virtual finish time based on its agent's weight, and the
heap always admits the smallest one next, so one agent flooding the pool
only pushes back its own future requests, never anyone else's. Second, the
live MaxOpenConns value isn't fixed, it's driven by an AIMD controller, the
same additive-increase/multiplicative-decrease congestion control that
makes TCP stable: grow the pool slowly while latency has headroom and
requests are actually queued, shrink it fast the moment measured latency
crosses your target. Layer a per-agent circuit breaker on top so an agent
whose queries keep failing gets fenced off for a cooldown instead of
retrying into an already-struggling database, and you get isolation,
self-tuning capacity, and failure containment out of one dependency-free
file.

Drop this into any Go service that hands out a *sql.DB to multiple AI
agents, background workers, or tenants and replace direct db.QueryContext
and db.ExecContext calls with pool.QueryContext and pool.ExecContext,
passing each caller's agent ID and an optional priority weight. Wire
pool.PrometheusText into your /metrics endpoint to watch queue depth,
per-agent activity, and breaker state without adding a client library, and
call pool.Snapshot anywhere you want the same data in code, for logging,
alerting, or an admin dashboard.
================================================================================
*/
