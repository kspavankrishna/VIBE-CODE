# Streaming Request Deduplicator

Five UI components ask for the same LLM stream at the same moment and you pay for five completions instead of one. This is a small TypeScript class that coalesces identical in-flight streaming requests behind a single signature so one upstream call fans out to many handlers.

**Language:** TypeScript | **Lines:** 124 | **Added:** 2026-04-08

## What this solves

Streaming APIs bill per request, not per consumer. A chat panel, a sidebar summary and a preview card can all mount within the same tick and all decide they need the same completion. Nothing in `fetch`, in an SSE client or in most data layers stops that. You get three HTTP connections, three sets of tokens billed and three slightly different partial states on screen because the streams arrive out of order. The bill is the loud symptom. The inconsistent UI is the one that generates bug reports.

Retries make it worse. A user hits send, the network stalls, they hit send again. Now two identical generations are running against the same prompt and the second one usually wins the race, so the first one is pure waste. Multiply that by a live product and the duplicate spend stops being a rounding error.

The third failure mode is concurrency itself. Without a ceiling on how many distinct streams can be open, a burst of traffic opens a burst of upstream connections. You hit the provider rate limit, get 429s and every user in the burst sees an error instead of the slow but correct answer. A hard cap that fails fast at the edge is better than a soft failure buried in provider errors.

This class attacks all three: identical requests share one stream, a signature that is already running never opens a second connection and a `maxConcurrent` limit stops the fan-out from becoming a stampede.

## Why I built it

Generic request deduplication libraries assume a request has one return value. They cache a promise, hand the same promise to every caller and resolve once. That model does not survive contact with streaming. A stream is not one value, it is a sequence, and a second subscriber arriving mid stream needs to be attached to the live iteration rather than handed a resolved promise. SWR style caches, in flight promise maps and most HTTP layers fall down here for the same reason.

The other gap is abort. Streaming clients hand you an `AbortSignal` and the natural instinct is to cancel when a component unmounts. If four components share one stream, one unmount must not kill the other three. Ownership of the signal has to live with the coalesced request, not with any individual subscriber. I wanted that in one readable file, not spread across three hooks.

## When to use it

- A React or Svelte page where several components independently request the same model completion on mount
- Users double clicking send, or a flaky connection producing retry storms against a per request billed API
- A collaborative editor where multiple clients in the same session request the same generated block
- Any expensive upstream where you need a hard ceiling on simultaneous open streams so you never trip a provider rate limit
- Debugging duplicate spend and you want a live `pending` and `completed` count to prove where it is coming from

## How it works

The core is `StreamingRequestDeduplicator`, holding two maps. `pending` maps a `RequestSignature`, just a string you compute from the prompt or query, to a `PendingRequest`. `completed` maps a signature to a small result marker. A `PendingRequest` carries a `Set<StreamHandler>` of subscriber callbacks, a `Set<OnComplete>` of finish callbacks and one `AbortController` owned by the coalesced request rather than by any single caller.

`deduplicate(signature, fn, handler, onComplete?)` is the entry point. It first looks up the signature in `pending`. On a hit it adds the new handler to the existing `handlers` set, adds `onComplete` if given and returns `attachToInFlight`. That is the whole coalescing trick: the late subscriber is added to a set the running loop is already iterating, so it starts receiving chunks from the next chunk onward without a second upstream call. On a miss it checks `pending.size` against `config.maxConcurrent` and throws immediately if the ceiling is reached, which is the fail fast admission control rather than a queue.

For a fresh signature it creates an `AbortController`, registers the `PendingRequest` and calls `fn(controller.signal)`. `fn` is an async generator factory, so the caller supplies the actual streaming call and receives the signal to wire into `fetch` or the provider SDK. The `for await` loop pulls each chunk and, for every chunk, iterates `request.handlers` and awaits each handler in turn. Iterating the live set on each chunk is what lets a subscriber that joined a moment ago start getting data. Awaiting each handler means back pressure from a slow consumer propagates to the whole fan out.

When the generator finishes, every `onComplete` callback fires, the signature is removed from `pending` and a `{ status: 'success' }` marker is written to `completed`. A `setTimeout` deletes that marker after `config.ttl`, default 30000 ms, which keeps `completed` from growing without bound. On a thrown error the catch block fans an `{ error, type: 'error' }` object out to every handler, removes the signature from `pending` and rethrows to the originating caller.

`abort(signature)` calls `controller.abort()` and drops the signature, cancelling the shared upstream stream for everyone attached. `status()` returns `{ pending, completed }` as raw map sizes, which is the observability hook you point a metric at. Config is normalised once in the constructor into `Required<DedupeConfig>`.

`attachToInFlight` is the weakest part and worth reading before you trust it. It returns a promise driven by a 100 ms `setInterval` poll plus a `ttl` length rejection timer. The poll checks `this.pending.has(request.controller.signal.toString())`, which is not the request signature, so the completion check does not resolve the way the rest of the design implies. Subscribers still receive chunks correctly through the shared handler set. It is the resolution of their returned promise that is unreliable.

## Usage

```ts
import { StreamingRequestDeduplicator, type DedupeConfig } from './StreamingRequestDeduplicator';

const dedupe = new StreamingRequestDeduplicator({ ttl: 30000, maxConcurrent: 50 });

async function* callModel(signal: AbortSignal) {
  const res = await fetch('/api/completions', {
    method: 'POST',
    body: JSON.stringify({ prompt: 'summarise this thread' }),
    signal
  });
  const reader = res.body!.getReader();
  const decoder = new TextDecoder();
  while (true) {
    const { done, value } = await reader.read();
    if (done) return;
    yield decoder.decode(value);
  }
}

const signature = 'summarise:thread-4471';

// Component A opens the stream
dedupe.deduplicate(
  signature,
  callModel,
  chunk => console.log('A', chunk),
  () => console.log('A done')
);

// Component B mounts a moment later and rides the same stream, no second API call
dedupe.deduplicate(signature, chunk => console.log('B', chunk));

console.log(dedupe.status()); // { pending: 1, completed: 0 }

// Cancel the shared stream for every subscriber
dedupe.abort(signature);
```

## Notes

- Signatures are yours to compute. The class never hashes the prompt or normalises arguments, so two logically identical requests with differently ordered JSON keys will not coalesce.
- Late subscribers get chunks from the moment they attach, not from the beginning. There is no chunk buffer, so a handler joining halfway through sees only the second half.
- The promise returned by `deduplicate` for a late subscriber goes through `attachToInFlight`, which polls a key derived from `controller.signal.toString()` rather than the signature. Treat that promise as unreliable and drive your UI from the handler callbacks and `onComplete` instead.
- The error path compares `error !== 'AbortError'`, a string comparison against a thrown value that is normally a `DOMException`, so an abort still fans an error object out to handlers.
- `completed` stores only `{ status: 'success' }`. It is a recency marker for observability, not a response cache. Nothing is replayed from it.
- Exceeding `maxConcurrent` throws synchronously with no queueing or backoff. If you want callers to wait rather than fail, wrap it.
- Handlers are awaited serially per chunk, so one slow handler throttles the whole fan out. Keep them cheap or hand off to a queue inside the handler.
- No dependencies beyond `AbortController`, `setTimeout` and async generators. Runs in any modern browser or Node 18 and up. No persistence, single process only, so a multi instance server dedupes per instance.
