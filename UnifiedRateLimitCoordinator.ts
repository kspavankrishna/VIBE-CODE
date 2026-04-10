type Resolver<T> = (value: T | PromiseLike<T>) => void;
type Rejecter = (reason?: unknown) => void;

interface LimitProfile {
  key: string;
  concurrency: number;
  reservoir: number;
  minSpacingMs?: number;
  refillAmount?: number;
  refillIntervalMs?: number;
  cooldownMs?: number;
}

interface AcquireOptions {
  weight?: number;
  priority?: number;
  signal?: AbortSignal;
  timeoutMs?: number;
  metadata?: Record<string, string | number | boolean>;
}

interface Lease {
  key: string;
  tokenId: string;
  acquiredAt: number;
  release: () => void;
}

interface ApiFeedback {
  statusCode?: number;
  retryAfterMs?: number;
  rateLimitResetAtMs?: number;
  observedRemaining?: number;
  observedLimit?: number;
}

interface QueueEntry {
  id: string;
  weight: number;
  priority: number;
  enqueuedAt: number;
  seq: number;
  signal?: AbortSignal;
  metadata?: Record<string, string | number | boolean>;
  timeoutHandle?: ReturnType<typeof setTimeout>;
  resolve: Resolver<Lease>;
  reject: Rejecter;
}

interface KeyState {
  profile: Required<Omit<LimitProfile, 'key'>> & { key: string };
  activeCount: number;
  tokens: number;
  lastRefillAt: number;
  lastDispatchAt: number;
  blockedUntil: number;
  queue: QueueEntry[];
  timer?: ReturnType<typeof setTimeout>;
  releasedLeases: Set<string>;
  recentWaitsMs: number[];
  deniedCount: number;
}

interface KeySnapshot {
  key: string;
  queued: number;
  activeCount: number;
  tokens: number;
  blockedUntil: number;
  avgRecentWaitMs: number;
  deniedCount: number;
}

class UnifiedRateLimitCoordinator {
  private readonly states = new Map<string, KeyState>();
  private sequence = 0;
  private leaseCounter = 0;
  private disposed = false;

  constructor(profiles: LimitProfile[] = []) {
    for (const profile of profiles) {
      this.registerProfile(profile);
    }
  }

  registerProfile(profile: LimitProfile): void {
    this.assertOpen();

    const normalized = this.normalizeProfile(profile);
    const existing = this.states.get(normalized.key);

    if (existing) {
      existing.profile = normalized;
      existing.tokens = Math.min(existing.tokens, normalized.reservoir);
      this.schedule(existing, 0);
      return;
    }

    this.states.set(normalized.key, {
      profile: normalized,
      activeCount: 0,
      tokens: normalized.reservoir,
      lastRefillAt: Date.now(),
      lastDispatchAt: 0,
      blockedUntil: 0,
      queue: [],
      releasedLeases: new Set<string>(),
      recentWaitsMs: [],
      deniedCount: 0
    });
  }

  async acquire(key: string, options: AcquireOptions = {}): Promise<Lease> {
    this.assertOpen();
    const state = this.getState(key);
    const weight = Math.max(1, Math.floor(options.weight ?? 1));
    const priority = options.priority ?? 0;

    if (weight > state.profile.reservoir) {
      throw new Error(`Request weight ${weight} exceeds reservoir ${state.profile.reservoir} for ${key}`);
    }

    return new Promise<Lease>((resolve, reject) => {
      const entry: QueueEntry = {
        id: `${key}:${++this.sequence}`,
        weight,
        priority,
        enqueuedAt: Date.now(),
        seq: this.sequence,
        signal: options.signal,
        metadata: options.metadata,
        resolve,
        reject
      };

      if (options.signal?.aborted) {
        reject(this.abortError('Acquire aborted before enqueue'));
        return;
      }

      if (options.timeoutMs && options.timeoutMs > 0) {
        entry.timeoutHandle = setTimeout(() => {
          this.removeEntry(state, entry.id);
          reject(new Error(`Timed out waiting for rate limit slot on ${key}`));
        }, options.timeoutMs);
      }

      if (options.signal) {
        options.signal.addEventListener(
          'abort',
          () => {
            this.removeEntry(state, entry.id);
            reject(this.abortError('Acquire aborted while queued'));
          },
          { once: true }
        );
      }

      state.queue.push(entry);
      this.sortQueue(state);
      this.schedule(state, 0);
    });
  }

  async run<T>(key: string, task: () => Promise<T> | T, options: AcquireOptions = {}): Promise<T> {
    const lease = await this.acquire(key, options);
    try {
      return await task();
    } finally {
      lease.release();
    }
  }

  applyFeedback(key: string, feedback: ApiFeedback): void {
    const state = this.getState(key);
    const now = Date.now();

    if (feedback.observedLimit && feedback.observedLimit > 0) {
      state.profile.reservoir = Math.max(1, Math.floor(feedback.observedLimit));
      state.tokens = Math.min(state.tokens, state.profile.reservoir);
    }

    if (typeof feedback.observedRemaining === 'number') {
      state.tokens = this.clamp(feedback.observedRemaining, 0, state.profile.reservoir);
    }

    if (feedback.retryAfterMs && feedback.retryAfterMs > 0) {
      state.blockedUntil = Math.max(state.blockedUntil, now + feedback.retryAfterMs);
      state.deniedCount += 1;
    } else if (feedback.rateLimitResetAtMs && feedback.rateLimitResetAtMs > now) {
      state.blockedUntil = Math.max(state.blockedUntil, feedback.rateLimitResetAtMs);
      state.deniedCount += 1;
    } else if (feedback.statusCode === 429) {
      state.blockedUntil = Math.max(state.blockedUntil, now + state.profile.cooldownMs);
      state.deniedCount += 1;
    }

    this.schedule(state, 0);
  }

  snapshot(): KeySnapshot[] {
    const now = Date.now();
    const rows: KeySnapshot[] = [];

    for (const state of this.states.values()) {
      this.refill(state, now);
      rows.push({
        key: state.profile.key,
        queued: state.queue.length,
        activeCount: state.activeCount,
        tokens: state.tokens,
        blockedUntil: state.blockedUntil,
        avgRecentWaitMs: this.average(state.recentWaitsMs),
        deniedCount: state.deniedCount
      });
    }

    return rows.sort((a, b) => a.key.localeCompare(b.key));
  }

  shutdown(reason = 'Coordinator shut down'): void {
    if (this.disposed) {
      return;
    }

    this.disposed = true;

    for (const state of this.states.values()) {
      if (state.timer) {
        clearTimeout(state.timer);
      }

      for (const entry of state.queue.splice(0)) {
        if (entry.timeoutHandle) {
          clearTimeout(entry.timeoutHandle);
        }
        entry.reject(new Error(reason));
      }
    }
  }

  private getState(key: string): KeyState {
    const state = this.states.get(key);
    if (!state) {
      throw new Error(`No rate limit profile registered for ${key}`);
    }
    return state;
  }

  private normalizeProfile(profile: LimitProfile): Required<Omit<LimitProfile, 'key'>> & { key: string } {
    if (!profile.key.trim()) {
      throw new Error('Profile key is required');
    }

    return {
      key: profile.key,
      concurrency: Math.max(1, Math.floor(profile.concurrency)),
      reservoir: Math.max(1, Math.floor(profile.reservoir)),
      minSpacingMs: Math.max(0, Math.floor(profile.minSpacingMs ?? 0)),
      refillAmount: Math.max(0, Math.floor(profile.refillAmount ?? profile.reservoir)),
      refillIntervalMs: Math.max(1, Math.floor(profile.refillIntervalMs ?? 60_000)),
      cooldownMs: Math.max(250, Math.floor(profile.cooldownMs ?? 2_000))
    };
  }

  private schedule(state: KeyState, delayMs: number): void {
    if (this.disposed) {
      return;
    }

    if (state.timer) {
      clearTimeout(state.timer);
    }

    state.timer = setTimeout(() => {
      state.timer = undefined;
      this.pump(state);
    }, Math.max(0, delayMs));
  }

  private pump(state: KeyState): void {
    if (this.disposed) {
      return;
    }

    const now = Date.now();
    this.refill(state, now);
    this.compactQueue(state);

    while (state.queue.length > 0) {
      const head = state.queue[0];
      const waitMs = this.requiredWaitMs(state, head, Date.now());

      if (waitMs > 0) {
        this.schedule(state, waitMs);
        return;
      }

      if (state.activeCount >= state.profile.concurrency) {
        return;
      }

      state.queue.shift();

      if (head.timeoutHandle) {
        clearTimeout(head.timeoutHandle);
      }

      state.tokens -= head.weight;
      state.activeCount += 1;
      state.lastDispatchAt = Date.now();
      this.pushWaitSample(state, state.lastDispatchAt - head.enqueuedAt);

      const lease = this.makeLease(state, head.id);
      head.resolve(lease);
      this.refill(state, Date.now());
    }
  }

  private makeLease(state: KeyState, leaseId: string): Lease {
    let released = false;
    const acquiredAt = Date.now();

    return {
      key: state.profile.key,
      tokenId: leaseId,
      acquiredAt,
      release: () => {
        if (released || this.disposed) {
          return;
        }

        released = true;

        if (state.releasedLeases.has(leaseId)) {
          return;
        }

        state.releasedLeases.add(leaseId);
        state.activeCount = Math.max(0, state.activeCount - 1);

        if (state.releasedLeases.size > 10_000) {
          state.releasedLeases.clear();
        }

        this.schedule(state, 0);
      }
    };
  }

  private requiredWaitMs(state: KeyState, entry: QueueEntry, now: number): number {
    this.refill(state, now);

    if (entry.signal?.aborted) {
      return 0;
    }

    if (state.activeCount >= state.profile.concurrency) {
      return 0;
    }

    const blockWait = Math.max(0, state.blockedUntil - now);
    const spacingWait = Math.max(0, state.lastDispatchAt + state.profile.minSpacingMs - now);

    if (state.tokens >= entry.weight) {
      return Math.max(blockWait, spacingWait);
    }

    const tokenWait = this.tokensReadyIn(state, entry.weight, now);
    return Math.max(blockWait, spacingWait, tokenWait);
  }

  private tokensReadyIn(state: KeyState, requiredWeight: number, now: number): number {
    if (state.tokens >= requiredWeight) {
      return 0;
    }

    if (state.profile.refillAmount <= 0) {
      return Number.MAX_SAFE_INTEGER;
    }

    const missing = requiredWeight - state.tokens;
    const batchesNeeded = Math.ceil(missing / state.profile.refillAmount);
    const nextBoundary = state.lastRefillAt + state.profile.refillIntervalMs;
    const firstWait = Math.max(0, nextBoundary - now);
    return firstWait + (batchesNeeded - 1) * state.profile.refillIntervalMs;
  }

  private refill(state: KeyState, now: number): void {
    const { refillAmount, refillIntervalMs, reservoir } = state.profile;
    if (refillAmount <= 0) {
      return;
    }

    const elapsed = now - state.lastRefillAt;
    if (elapsed < refillIntervalMs) {
      return;
    }

    const intervals = Math.floor(elapsed / refillIntervalMs);
    state.tokens = Math.min(reservoir, state.tokens + intervals * refillAmount);
    state.lastRefillAt += intervals * refillIntervalMs;
  }

  private sortQueue(state: KeyState): void {
    state.queue.sort((a, b) => {
      if (a.priority !== b.priority) {
        return b.priority - a.priority;
      }
      if (a.weight !== b.weight) {
        return a.weight - b.weight;
      }
      return a.seq - b.seq;
    });
  }

  private compactQueue(state: KeyState): void {
    if (state.queue.length === 0) {
      return;
    }

    state.queue = state.queue.filter((entry) => {
      if (entry.signal?.aborted) {
        if (entry.timeoutHandle) {
          clearTimeout(entry.timeoutHandle);
        }
        entry.reject(this.abortError('Acquire aborted while queued'));
        return false;
      }
      return true;
    });
  }

  private removeEntry(state: KeyState, entryId: string): void {
    const index = state.queue.findIndex((entry) => entry.id === entryId);
    if (index < 0) {
      return;
    }

    const [removed] = state.queue.splice(index, 1);
    if (removed.timeoutHandle) {
      clearTimeout(removed.timeoutHandle);
    }
  }

  private pushWaitSample(state: KeyState, waitMs: number): void {
    state.recentWaitsMs.push(waitMs);
    if (state.recentWaitsMs.length > 50) {
      state.recentWaitsMs.shift();
    }
  }

  private average(values: number[]): number {
    if (values.length === 0) {
      return 0;
    }
    return Math.round(values.reduce((sum, value) => sum + value, 0) / values.length);
  }

  private clamp(value: number, min: number, max: number): number {
    return Math.max(min, Math.min(max, Math.floor(value)));
  }

  private abortError(message: string): Error {
    const error = new Error(message);
    error.name = 'AbortError';
    return error;
  }

  private assertOpen(): void {
    if (this.disposed) {
      throw new Error('Coordinator is shut down');
    }
  }
}

export {
  UnifiedRateLimitCoordinator,
  type AcquireOptions,
  type ApiFeedback,
  type KeySnapshot,
  type Lease,
  type LimitProfile
};

/*
UnifiedRateLimitCoordinator gives one place to control API pressure across several keys or providers. It queues work, respects concurrency, spacing, token budgets, and backs off when the server says slow down. This helps when your app fans out to LLMs, embeddings, and search APIs at the same time. Instead of spreading rate limit logic across many callers, you put it here and get predictable behavior plus useful snapshots.
*/
