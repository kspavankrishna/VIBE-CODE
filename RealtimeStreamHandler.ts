import Anthropic from "@anthropic-ai/sdk";

async function streamWithBackpressure(
  prompt: string,
  onChunk: (chunk: string) => void
) {
  const client = new Anthropic();
  const stream = await client.messages.stream({
    model: "claude-3-5-sonnet-20241022",
    max_tokens: 1024,
    messages: [{ role: "user", content: prompt }],
  });

  for await (const chunk of stream) {
    if (
      chunk.type === "content_block_delta" &&
      chunk.delta.type === "text_delta"
    ) {
      onChunk(chunk.delta.text);
    }
  }

  return stream.finalMessage;
}

async function parallelStreams(prompts: string[]) {
  const results = await Promise.all(
    prompts.map(
      (p) =>
        new Promise<string>((resolve) => {
          let text = "";
          streamWithBackpressure(p, (chunk) => {
            text += chunk;
          }).then(() => resolve(text));
        })
    )
  );
  return results;
}

export { streamWithBackpressure, parallelStreams };
