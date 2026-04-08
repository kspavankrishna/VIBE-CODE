type RequestSignature = string;
type StreamHandler = (chunk: any) => void | Promise<void>;
type OnComplete = () => void;

interface PendingRequest {
  handlers: Set<StreamHandler>;
  onComplete: Set<OnComplete>;
  controller: AbortController;
}

interface DedupeConfig {
  ttl?: number;
  maxConcurrent?: number;
}

class StreamingRequestDeduplicator {
  private pending = new Map<RequestSignature, PendingRequest>();
  private completed = new Map<RequestSignature, any>();
  private config: Required<DedupeConfig>;

  constructor(config: DedupeConfig = {}) {
    this.config = {
      ttl: config.ttl ?? 30000,
      maxConcurrent: config.maxConcurrent ?? 50
    };
  }

  async deduplicate(
    signature: RequestSignature,
    fn: (signal: AbortSignal) => AsyncGenerator<any>,
    handler: StreamHandler,
    onComplete?: OnComplete
  ): Promise<void> {
    const existing = this.pending.get(signature);

    if (existing) {
      existing.handlers.add(handler);
      if (onComplete) existing.onComplete.add(onComplete);
      return this.attachToInFlight(existing, handler, onComplete);
    }

    if (this.pending.size >= this.config.maxConcurrent) {
      throw new Error(`Max concurrent requests (${this.config.maxConcurrent}) reached`);
    }

    const controller = new AbortController();
    const request: PendingRequest = {
      handlers: new Set([handler]),
      onComplete: onComplete ? new Set([onComplete]) : new Set(),
      controller
    };

    this.pending.set(signature, request);

    try {
      const generator = fn(controller.signal);
      for await (const chunk of generator) {
        for (const h of request.handlers) {
          await h(chunk);
        }
      }

      for (const cb of request.onComplete) {
        cb();
      }

      this.pending.delete(signature);
      this.completed.set(signature, { status: 'success' });
      setTimeout(() => this.completed.delete(signature), this.config.ttl);
    } catch (error) {
      if (error !== 'AbortError') {
        for (const h of request.handlers) {
          await h({ error, type: 'error' });
        }
      }
      this.pending.delete(signature);
      throw error;
    }
  }

  private attachToInFlight(
    request: PendingRequest,
    handler: StreamHandler,
    onComplete?: OnComplete
  ): Promise<void> {
    return new Promise((resolve, reject) => {
      const checkInterval = setInterval(() => {
        if (!this.pending.has(request.controller.signal.toString())) {
          clearInterval(checkInterval);
          resolve();
        }
      }, 100);

      setTimeout(() => {
        clearInterval(checkInterval);
        reject(new Error('Request timeout'));
      }, this.config.ttl);
    });
  }

  abort(signature: RequestSignature): void {
    const request = this.pending.get(signature);
    if (request) {
      request.controller.abort();
      this.pending.delete(signature);
    }
  }

  status(): { pending: number; completed: number } {
    return {
      pending: this.pending.size,
      completed: this.completed.size
    };
  }
}

export { StreamingRequestDeduplicator, type DedupeConfig };

/*
================================================================================
EXPLANATION
StreamingRequestDeduplicator prevents duplicate API calls when multiple handlers subscribe to the same request. Built because LLM streaming APIs charge per request, not per consumer—duplicate calls blow budgets. Use it when users retry, network flakes, or you have multiple UI components requesting the same data stream. The trick: signature-based deduplication with request coalescing—multiple handlers attach to a single in-flight stream. Drop into real-time AI apps, collaborative editors, or any system where concurrent requests to expensive APIs need consolidation. Handles abort signals and tracks in-flight vs completed for observability.
================================================================================
*/
