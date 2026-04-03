import anthropic
import time
from typing import Generator

def streaming_generator(prompt: str, model: str = "claude-3-5-sonnet-20241022") -> Generator[str, None, None]:
    client = anthropic.Anthropic()
    with client.messages.stream(
        model=model,
        max_tokens=2048,
        messages=[{"role": "user", "content": prompt}]
    ) as stream:
        for text in stream.text_stream:
            yield text

def token_aware_chunker(prompt: str, chunk_size: int = 100) -> Generator[str, None, None]:
    buffer = ""
    for chunk in streaming_generator(prompt):
        buffer += chunk
        if len(buffer) >= chunk_size:
            yield buffer[:chunk_size]
            buffer = buffer[chunk_size:]
    if buffer:
        yield buffer

def rate_limited_stream(prompt: str, delay: float = 0.01) -> Generator[str, None, None]:
    for chunk in streaming_generator(prompt):
        yield chunk
        time.sleep(delay)

if __name__ == "__main__":
    for chunk in token_aware_chunker("Write a function that calculates fibonacci"):
        print(chunk, end="", flush=True)

# === EXPLANATION ===
# This module provides production-grade streaming utilities for LLM responses.
# Built because real applications need fine-grained control over token flow —
# buffering chunks into meaningful units, rate-limiting to avoid overwhelming 
# downstream systems, and handling backpressure gracefully. token_aware_chunker
# groups streaming output into configurable sizes for batch processing or UI updates.
# rate_limited_stream prevents callback floods in event-driven architectures. Use this
# when you're integrating LLM streams into web servers, message queues, or real-time
# frontends where naive streaming breaks the contract. The generator pattern keeps
# memory footprint constant regardless of total output length.
