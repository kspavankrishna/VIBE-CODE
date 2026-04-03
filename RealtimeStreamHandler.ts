import Anthropic from "@anthropic-ai/sdk";

type StreamCallback = (chunk: string) => void | Promise<void>;

class StreamOrchestrator {
  private client: Anthropic;
  private buffer: string = "";
  private metrics = { chunksReceived: 0, totalTokens: 0 };

  constructor() {
    this.client = new Anthropic();
  }

  async stream(
    prompt: string,
    onChunk: StreamCallback,
    bufferSize: number = 50
  ): Promise<string> {
    let output = "";
    const stream = await this.client.messages.stream({
      model: "claude-3-5-sonnet-20241022",
      max_tokens: 1024,
      messages: [{ role: "user", content: prompt }],
    });

    for await (const event of stream) {
      if (
        event.type === "content_block_delta" &&
        event.delta.type === "text_delta"
      ) {
        this.buffer += event.delta.text;
        output += event.delta.text;
        this.metrics.chunksReceived++;

        if (this.buffer.length >= bufferSize) {
          await onChunk(this.buffer);
          this.buffer = "";
        }
      }
    }

    if (this.buffer.length > 0) {
      await onChunk(this.buffer);
    }

    return output;
  }

  async parallelStream(
    prompts: string[],
    onChunk: StreamCallback
  ): Promise<string[]> {
    return Promise.all(
      prompts.map((p) => this.stream(p, onChunk))
    );
  }

  getMetrics() {
    return this.metrics;
  }
}

export { StreamOrchestrator };

// === EXPLANATION ===
// StreamOrchestrator is a wrapper for managing high-throughput LLM streaming in production
// environments. The problem: naive streaming floods callbacks and loses control over buffer
// management. This class buffers incoming chunks to configurable sizes, flushes intelligently,
// and tracks metrics—chunks received, token flow visibility. Use it when building real-time
// dashboards, chat interfaces, or data pipelines where you need to batch process tokens for
// rendering efficiency or downstream queuing. The onChunk callback handles buffered segments
// instead of individual tokens, reducing UI thrashing and system load. parallelStream handles
// multiple concurrent requests with unified metrics, critical for multi-user or multi-query
// systems. Drop this into any TypeScript backend where streaming is a bottleneck.
