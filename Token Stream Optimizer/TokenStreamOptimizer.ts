import { EventEmitter } from 'events';

interface TokenBatch { tokens: string[], timestamp: number, count: number }
interface RateLimitConfig { tokensPerSecond: number, burst: number }

class TokenStreamOptimizer extends EventEmitter {
  private tokenQueue: TokenBatch[] = [];
  private circuitOpen = false;
  private lastTokenTime = 0;
  private allowedTokens: number;
  private readonly config: RateLimitConfig;
  private tokenCount = 0;
  private estimatedCost = 0;

  constructor(config: RateLimitConfig = { tokensPerSecond: 100, burst: 10 }) {
    super();
    this.config = config;
    this.allowedTokens = config.burst;
  }

  private tokenize(text: string): string[] {
    return text.match(/\b\w+\b|[^\w\s]/g) || [];
  }

  async streamTokens(
    text: string,
    chunkSize: number = 5,
    delayMs: number = 50
  ): Promise<void> {
    if (this.circuitOpen) throw new Error('Circuit breaker open');

    const tokens = this.tokenize(text);
    let processed = 0;

    for (let i = 0; i < tokens.length; i += chunkSize) {
      const batch = tokens.slice(i, i + chunkSize);
      const available = this.checkRateLimit(batch.length);

      if (!available) {
        await new Promise(resolve => setTimeout(resolve, delayMs));
        this.refillTokens();
      }

      this.emit('tokens', batch);
      this.tokenCount += batch.length;
      processed++;

      if (processed % 20 === 0) this.emit('progress', { processed, total: tokens.length });
    }

    this.emit('complete', { tokenCount: this.tokenCount, estimated_cost_usd: this.estimateCost() });
  }

  private checkRateLimit(tokensNeeded: number): boolean {
    const now = Date.now();
    const timeSinceLastReset = (now - this.lastTokenTime) / 1000;

    if (timeSinceLastReset > 1) {
      this.refillTokens();
      this.lastTokenTime = now;
    }

    if (this.allowedTokens >= tokensNeeded) {
      this.allowedTokens -= tokensNeeded;
      return true;
    }

    return false;
  }

  private refillTokens(): void {
    this.allowedTokens = Math.min(
      this.allowedTokens + (this.config.tokensPerSecond / 1000),
      this.config.burst
    );
  }

  private estimateCost(): number {
    const inputCost = (this.tokenCount / 1000) * 0.0001;
    return Math.round(inputCost * 10000) / 10000;
  }

  setCircuitBreaker(open: boolean): void {
    this.circuitOpen = open;
    if (open) this.emit('circuit-open');
  }

  getMetrics() {
    return { tokenCount: this.tokenCount, estimatedCost: this.estimateCost(), rateLimitRemaining: this.allowedTokens };
  }
}

export default TokenStreamOptimizer;
