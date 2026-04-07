import { EventEmitter } from "events";

interface StreamConfig {
  contextLimit: number;
  warningThreshold: number;
  overflowMargin: number;
}

interface StreamMetrics {
  tokensUsed: number;
  tokensRemaining: number;
  estimatedTokensNext: number;
  willOverflow: boolean;
  overflowIn: number;
}

class StreamingContextPredictor extends EventEmitter {
  private contextLimit: number;
  private warningThreshold: number;
  private overflowMargin: number;
  private tokensUsed: number = 0;
  private tokenHistory: number[] = [];
  private isStreaming: boolean = false;

  constructor(config: StreamConfig) {
    super();
    this.contextLimit = config.contextLimit;
    this.warningThreshold = config.warningThreshold;
    this.overflowMargin = config.overflowMargin;
  }

  trackTokens(count: number): StreamMetrics {
    this.tokensUsed += count;
    this.tokenHistory.push(count);

    if (this.tokenHistory.length > 10) {
      this.tokenHistory.shift();
    }

    const avgTokens = this.getAverageTokenRate();
    const estimatedNext = Math.ceil(avgTokens * 1.2);
    const tokensRemaining = this.contextLimit - this.tokensUsed;
    const willOverflow = tokensRemaining - estimatedNext < this.overflowMargin;

    const metrics: StreamMetrics = {
      tokensUsed: this.tokensUsed,
      tokensRemaining,
      estimatedTokensNext: estimatedNext,
      willOverflow,
      overflowIn: Math.max(0, tokensRemaining - estimatedNext),
    };

    if (this.tokensUsed >= this.contextLimit * this.warningThreshold) {
      this.emit("warning:approaching-limit", metrics);
    }

    if (willOverflow) {
      this.emit("critical:overflow-predicted", metrics);
      this.reset();
    }

    return metrics;
  }

  private getAverageTokenRate(): number {
    if (this.tokenHistory.length === 0) return 0;
    return this.tokenHistory.reduce((a, b) => a + b, 0) / this.tokenHistory.length;
  }

  predictOverflow(): { willOverflow: boolean; tokensUntilOverflow: number } {
    const avgRate = this.getAverageTokenRate();
    const projectedUsage = this.tokensUsed + avgRate;
    return {
      willOverflow: projectedUsage >= this.contextLimit - this.overflowMargin,
      tokensUntilOverflow: Math.max(0, this.contextLimit - projectedUsage),
    };
  }

  reset(): void {
    this.tokensUsed = 0;
    this.tokenHistory = [];
    this.emit("stream:reset");
  }

  getMetrics(): StreamMetrics {
    const avgTokens = this.getAverageTokenRate();
    const estimatedNext = Math.ceil(avgTokens * 1.2);
    const tokensRemaining = this.contextLimit - this.tokensUsed;

    return {
      tokensUsed: this.tokensUsed,
      tokensRemaining,
      estimatedTokensNext: estimatedNext,
      willOverflow: tokensRemaining < estimatedNext + this.overflowMargin,
      overflowIn: Math.max(0, tokensRemaining - estimatedNext),
    };
  }
}

export default StreamingContextPredictor;

/*
================================================================================
EXPLANATION
StreamingContextPredictor prevents context overflow during streaming by predicting when tokens will fill the window. It tracks token usage, calculates average token rate from recent chunks, and triggers warnings before overflow hits. Built because streaming LLM responses are unpredictable — you don't know token count until it's streamed. Use it when building chat UIs, long-form generation, or any system needing graceful context transitions. The trick: maintain a rolling average of token chunks and project ahead 20% for safety. Emits events before catastrophic overflow so you can start a new stream, checkpoint context, or gracefully degrade. Drop into your streaming handler to prevent silent context truncation.
================================================================================
*/
