# AI Stream SSE Reader

Server Sent Events coming out of AI APIs are not the tidy one line per event stream most .NET code assumes. This is a single file C# SSE parser that joins multi line data fields, skips heartbeat comments, honours the `[DONE]` sentinel and refuses to let one stuck stream eat your heap.

**Language:** C# | **Lines:** 97 | **Added:** 2026-04-14

## What this solves

The most common streaming bug in .NET services is a substring. Someone reads a line, checks whether it starts with `data: `, chops six characters off the front and throws the rest at `JsonSerializer`. That works against a curl transcript and fails against real provider traffic, because the SSE wire format lets one event carry several `data:` lines that must be joined with newlines before the payload means anything. When a provider or a proxy splits a JSON chunk across two data lines, the naive reader deserializes two broken halves, throws, and the user watching the token stream sees the answer stop mid sentence. Nobody can reproduce it either, because the split depends on buffer boundaries upstream.

The second failure is heartbeats. Load balancers, reverse proxies and MCP relays keep long lived connections alive with comment lines that begin with a colon, things like `: ping` every fifteen seconds. A reader that assumes every line is a payload chokes on them. The third failure is the terminator. OpenAI compatible endpoints close a stream with `data: [DONE]`, which is not JSON. Teams hit that, wrap the deserializer in a broad try/catch, and from then on every genuine parse error is silently swallowed too. That is the version that costs you a week when the provider renames a field.

Then there is memory. If the upstream stalls after a partial event and never writes the blank line that terminates it, an unbounded `StringBuilder` grows for as long as the socket stays open. On a gateway fanning out dozens of concurrent model streams that is a worker process OOM, and it lands at peak load because that is when a provider is most likely to hang. Metadata gets dropped too. The `id:` field is what you send back as `Last-Event-ID` to resume a stream and `retry:` is the server telling you how long to wait before reconnecting, so readers that watch only `data:` leave reconnect logic guessing with a hardcoded delay.

## Why I built it

I kept seeing .NET services read one line and hope for the best. The parsers that do exist are usually buried inside a provider SDK, so you get correct parsing only if you also adopt that SDK's HTTP stack, auth, models and retry policy. That is no help when the thing you are reading is your own gateway, an MCP relay or an internal log stream that happens to speak SSE.

I wanted the opposite shape: a `Stream` goes in, complete `SseEvent` records come out, no dependencies, no HTTP opinions, small enough that another engineer can read the whole thing before trusting it. This is that file.

## When to use it

- Calling an OpenAI compatible chat completions endpoint from a .NET worker and needing whole JSON chunks, not fragments
- Running your own gateway in front of a model provider and re-emitting SSE downstream
- Consuming an MCP server over an HTTP transport that streams events back
- Tailing a progress or log stream that uses SSE and pads it with comment heartbeats
- Blazor or desktop clients where a wedged stream must not quietly grow the heap
- Writing deterministic tests against a canned SSE fixture, since it takes any readable `Stream`

## How it works

The public surface is one method: `AiStreamSseReader.ReadAsync(Stream stream, int maxEventChars = 128_000, CancellationToken cancellationToken = default)`, an `async IAsyncEnumerable<SseEvent>` marked with `[EnumeratorCancellation]` so the token flows through `WithCancellation` at the call site. Before the first read it throws on a null stream, on a stream that cannot be read and on a cap that is zero or negative. Events come back as `public sealed record SseEvent(string Event, string Data, string? Id, TimeSpan? Retry)`.

Reading goes through a `StreamReader` built with UTF8, byte order mark detection on, a 4096 byte buffer and `leaveOpen: true`, so it never closes a stream it did not open. The loop is `while (await reader.ReadLineAsync().WaitAsync(cancellationToken) is { } line)`, a pattern match on the nullable result where `null` ends the enumeration. `Task.WaitAsync` is what makes the wait cancellable.

Parsing follows the SSE field grammar rather than a prefix check. An empty line is the event boundary and triggers a dispatch. A line whose first character is `:` is a comment and is skipped, which is what makes heartbeats harmless. Otherwise the line is split at the first colon: everything before it is the field name, everything after is the value, and one leading space on the value is stripped. A line with no colon is a field name with an empty value. Four field names are acted on and the rest ignored. `event` sets the event name, falling back to `message` when blank. `data` appends to a `StringBuilder`, inserting a newline first if the buffer already has content, which is the multi line join naive readers get wrong. `id` is captured as `lastEventId` unless the value contains a NUL character, which the spec says to ignore. `retry` is parsed as milliseconds and stored as a `TimeSpan` when it parses and is not negative.

Dispatch is `TryDispatch`, which takes `eventName` by reference. If the data buffer is empty it resets the name to `message` and returns false, so a block carrying only an `event:` or `id:` line produces nothing. Otherwise it builds the `SseEvent`, resets the name and clears the buffer. Note what is deliberately not reset: `lastEventId` and `retry` persist across events, so the id on an event is the last id the server sent, which is the value you would echo back in a `Last-Event-ID` header.

Two things guard the tail. The `[DONE]` sentinel is checked with an ordinal comparison against the assembled data, and on a match the method does `yield break` instead of returning it, so the terminator never reaches your deserializer. Size is bounded by `EnsureWithinLimit`, called after every `data` append, which throws `InvalidDataException` once the buffer passes `maxEventChars`. That is the backstop against an upstream that streams forever without writing a blank line. When the stream ends, a final `TryDispatch` flushes any block that never got its blank line, again suppressing `[DONE]`.

## Usage

```csharp
using System.Net.Http.Json;
using System.Text.Json;
using VibeCode;

using var http = new HttpClient();
using var request = new HttpRequestMessage(HttpMethod.Post, "https://api.example.com/v1/chat/completions")
{
    Content = JsonContent.Create(new { model = "your-model", stream = true, messages })
};

using var response = await http.SendAsync(
    request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
response.EnsureSuccessStatusCode();

await using var body = await response.Content.ReadAsStreamAsync(cancellationToken);

try
{
    await foreach (var sse in AiStreamSseReader.ReadAsync(body, cancellationToken: cancellationToken))
    {
        // sse.Event  -> "message" unless the server sent an event: line
        // sse.Data   -> all data: lines for this block, joined with '\n'
        // sse.Id     -> last id: seen, for Last-Event-ID on reconnect
        // sse.Retry  -> reconnect hint from a retry: line, if any

        if (sse.Event == "error") { Log(sse.Data); break; }

        using var chunk = JsonDocument.Parse(sse.Data);
        Console.Write(chunk.RootElement
            .GetProperty("choices")[0].GetProperty("delta")
            .GetProperty("content").GetString());
    }
}
catch (InvalidDataException ex)
{
    // an event grew past maxEventChars, upstream is almost certainly wedged
    Log(ex.Message);
}

// tighter cap for an untrusted or internal relay
await foreach (var sse in AiStreamSseReader.ReadAsync(body, maxEventChars: 16_000, cancellationToken: ct))
{
    Handle(sse);
}
```

## Notes

- Needs .NET 8 or later. `ArgumentOutOfRangeException.ThrowIfNegativeOrZero` is .NET 8, `Task.WaitAsync` is .NET 6 and the file scoped namespace needs C# 10. No NuGet packages.
- Built with `leaveOpen: true`, so disposing the stream is the caller's job. The file does no HTTP of its own.
- Cancellation is `WaitAsync` around `ReadLineAsync`. That abandons the await promptly but does not cancel the underlying socket read, so dispose the response stream to tear the connection down.
- A trailing block never closed by a blank line is still emitted at end of stream. Browser `EventSource` discards it, so this is deliberately more forgiving than the spec.
- Hitting `[DONE]` stops enumeration immediately. Anything the server writes after it is never read, and the stream is left mid position.
- `retry` is sticky: once seen it is attached to every later event until the server sends a different one. Same for `id`, which is per spec.
- `maxEventChars` counts the data buffer only and is checked after each append, so it can overshoot by one line before `InvalidDataException` is thrown. Event names, ids and comments are not counted.
- No reconnect, backoff or `Last-Event-ID` resend logic. It hands you the id and the retry hint so you can build that yourself. Unrecognised field names are ignored, so a provider extension field never reaches your code.
