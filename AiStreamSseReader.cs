using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace VibeCode;

public sealed record SseEvent(string Event, string Data, string? Id, TimeSpan? Retry);

public static class AiStreamSseReader
{
    public static async IAsyncEnumerable<SseEvent> ReadAsync(
        Stream stream,
        int maxEventChars = 128_000,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);
        if (!stream.CanRead) throw new ArgumentException("Stream must be readable.", nameof(stream));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxEventChars);
        using var reader = new StreamReader(stream, Encoding.UTF8, true, 4096, leaveOpen: true);
        var eventName = "message";
        string? lastEventId = null;
        TimeSpan? retry = null;
        var data = new StringBuilder();

        while (await reader.ReadLineAsync().WaitAsync(cancellationToken) is { } line)
        {
            if (line.Length == 0)
            {
                if (TryDispatch(ref eventName, lastEventId, retry, data, out var sseEvent))
                {
                    if (string.Equals(sseEvent.Data, "[DONE]", StringComparison.Ordinal)) yield break;
                    yield return sseEvent;
                }
                continue;
            }
            if (line[0] == ':') continue;

            var separator = line.IndexOf(':');
            var field = separator < 0 ? line : line[..separator];
            var value = separator < 0 ? string.Empty : line[(separator + 1)..];
            if (value.StartsWith(' ')) value = value[1..];

            switch (field)
            {
                case "event":
                    eventName = string.IsNullOrWhiteSpace(value) ? "message" : value;
                    break;
                case "data":
                    if (data.Length > 0) data.Append('\n');
                    data.Append(value);
                    EnsureWithinLimit(data.Length, maxEventChars);
                    break;
                case "id":
                    if (!value.Contains('\0')) lastEventId = value;
                    break;
                case "retry":
                    if (int.TryParse(value, out var milliseconds) && milliseconds >= 0) retry = TimeSpan.FromMilliseconds(milliseconds);
                    break;
            }
        }

        if (TryDispatch(ref eventName, lastEventId, retry, data, out var finalEvent) && !string.Equals(finalEvent.Data, "[DONE]", StringComparison.Ordinal))
            yield return finalEvent;
    }

    private static bool TryDispatch(
        ref string eventName,
        string? lastEventId,
        TimeSpan? retry,
        StringBuilder data,
        out SseEvent sseEvent)
    {
        if (data.Length == 0)
        {
            eventName = "message";
            sseEvent = default!;
            return false;
        }
        sseEvent = new SseEvent(eventName, data.ToString(), lastEventId, retry);
        eventName = "message";
        data.Clear();
        return true;
    }

    private static void EnsureWithinLimit(int length, int limit)
    {
        if (length > limit) throw new InvalidDataException($"SSE event exceeded {limit} characters.");
    }
}

/*
This solves the annoying failure mode where AI APIs, MCP relays, and log streams all claim SSE but then send multi-line data chunks, comments, retry hints, and a final [DONE] marker that naive readers mishandle. Built because I kept seeing .NET services read one line and hope for the best, which breaks on real provider traffic. Use it when you need a small reader you can trust before your own app logic starts. The trick: it joins repeated data lines correctly, keeps the last event id around, respects retry hints, and hard-stops oversized frames before one noisy stream eats memory. Drop this into any .NET worker, API gateway, desktop client, or internal SDK that consumes streaming responses.
*/
