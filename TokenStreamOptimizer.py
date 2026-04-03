import anthropic

def stream_optimized(prompt: str, model: str = "claude-3-5-sonnet-20241022") -> str:
    client = anthropic.Anthropic()
    result = ""
    
    with client.messages.stream(
        model=model,
        max_tokens=2048,
        messages=[{"role": "user", "content": prompt}]
    ) as stream:
        for text in stream.text_stream:
            result += text
    
    return result

def batch_process(prompts: list[str]) -> list[str]:
    return [stream_optimized(p) for p in prompts]

if __name__ == "__main__":
    test = "Explain quantum computing in one sentence"
    print(stream_optimized(test))
