# Semantic Pixel Renderer

Generative visuals on the web almost always mean a round trip to an image API: a network call, a bill, a rate limit and a different picture every time you ask for the same thing. This is a dependency free JavaScript class that turns a text prompt into a deterministic canvas image on the client, with no API key and no network.

**Language:** JavaScript | **Lines:** 96 | **Added:** 2026-04-05

## What this solves

The failure mode is the external image API sitting in your render path. You ship a page that generates a background, an avatar placeholder or a card header from a string. Every one of those is an HTTP call to a hosted model. In development it feels fine. In production you find out what it costs: latency in seconds, a per image price, a requests per minute ceiling and a hard dependency on someone else's uptime. When that service degrades, the person who notices first is the user staring at a blank hero section.

The second failure is nondeterminism. Hosted models do not guarantee that the same prompt returns the same image. That breaks caching, it breaks visual regression tests and it breaks any feature where the picture is an identity. If a user's texture comes from their username, it has to be identical on every device on every visit. A sampling based model gives you that only if you store the output, and now you are running an image bucket and a cleanup job for decoration.

Third is the offline and privacy case. Anything that calls out sends the prompt to a third party. If the prompt is a customer name or an internal project code, that is data leaving your boundary for a decorative pixel field. In an air gapped or offline first app the call just fails.

This file removes all three. Rendering is a pure function of the prompt plus the width and height. Same input, same pixels, computed in the browser in one pass over the pixel buffer. No key, no quota, no request.

## Why I built it

The middle ground did not exist. On one side, hosted text to image models that are powerful, slow and metered. On the other, generic noise libraries: simplex, perlin, seedable PRNGs. Those are deterministic and fast, but they take a numeric seed, so wiring text into them means writing your own string to seed step, your own colour mapping and your own cache. Everybody rewrites that glue and everybody rewrites it slightly wrong.

I wanted one small file where the input is the string a human already has, the output goes straight onto a canvas and the whole thing is auditable in one sitting. No build step, no package to vet, no transitive dependencies.

## When to use it

- A generative background or hero texture that has to look identical on every reload and on every device.
- Deterministic placeholder art keyed to a username, an order id or a document title, without storing a generated file per user.
- Offline or air gapped apps where an image API call would simply fail.
- Visual regression tests that need byte stable output, so a screenshot diff means a real change and not model drift.
- Creative coding sketches where you want text driven colour fields with no dependency and no API key.

## How it works

The public surface is one ES module default export, the class `SemanticPixelRenderer`. The constructor takes a canvas element and a `modelEndpoint`, grabs a `2d` context and initialises a `Map` called `cache` plus a `queue` array and a `rendering` boolean. Note up front: `modelEndpoint`, `queue` and `rendering` are stored and never read anywhere in the file. Nothing here makes a network request.

`renderFromPrompt(prompt, width = 512, height = 512)` is the entry point. It builds a cache key of the form `prompt-WIDTHxHEIGHT` and, on a hit, hands the stored record to `drawPixels`, which calls `putImageData` and returns. On a miss it sets the canvas dimensions, allocates an `ImageData` via `createImageData` and walks the backing `Uint8ClampedArray` four bytes at a time.

The seed is the semantic hashing step. The prompt is split on `/\s+/`, lowercased per token, then reduced by summing `charCodeAt(0)` of each token. That is the whole text to number bridge: only the first character of each word contributes, and addition is commutative, so word order does not matter. Cheap and stable, which is the point, but it is not a content hash. See the notes.

Per pixel, the index becomes `x` and `y` with a modulo and a floor, then feeds a spatial hash: `(seed + x * 73856093 ^ y * 19349663) >>> 0`. Those constants are the large primes from the Teschner spatial hashing scheme, chosen because multiplying coordinates by co prime large values scatters neighbouring cells into unrelated buckets. Operator precedence matters: `+` binds tighter than `^`, so it evaluates as `((seed + x * 73856093) ^ (y * 19349663)) >>> 0`. The XOR forces both sides through ToInt32 and the unsigned shift yields a non negative 32 bit integer. The products overflow 32 bits at any realistic width, which is fine: the wrap is exact because the doubles stay well under 2^53.

Colour comes from slicing different bit ranges of that one hash, so the channels are decorrelated. Hue is `hash % 360`, saturation is `50 + ((hash >> 8) % 50)` and lightness is `40 + ((hash >> 16) % 40)`. The clamps are deliberate: saturation stays in 50 to 99 and lightness in 40 to 79, which keeps the field vivid and avoids washed out or near black pixels. `hslToRgb` normalises the values and runs the standard conversion, computing `q` and `p` and calling `hueToRgb` at hue offsets of `+1/3`, `0` and `-1/3`. Alpha is a constant 255.

After the loop the buffer goes to the canvas with `putImageData`, and the cache stores `{ data: imageData, prompt, seed }` under the key. That memoisation makes repeat prompts effectively free: the second call skips the per pixel loop and does a single blit.

What comes out is a deterministic high frequency colour field, not an object or a scene. The hash decorrelates adjacent pixels by design, so the result reads as structured noise whose palette shifts with the prompt. Treat it as procedural texture keyed by text, not as a diffusion model.

## Usage

```js
import SemanticPixelRenderer from "./SemanticPixelRenderer.js";

const canvas = document.querySelector("#art");

// second argument is stored but currently unused, pass null
const renderer = new SemanticPixelRenderer(canvas, null);

// first call runs the pixel loop and caches the result
const imageData = await renderer.renderFromPrompt("sunset over cold water", 512, 512);

// identical prompt and size, served from the cache, no loop
await renderer.renderFromPrompt("sunset over cold water", 512, 512);

// defaults are 512 x 512
await renderer.renderFromPrompt("quarterly report q3");

// export the canvas if you want a file
const png = canvas.toDataURL("image/png");
```

## Notes

- The seed only reads `charCodeAt(0)` of each token and sums them, so it is order independent and collides easily. "cat dog" and "car door" produce the same image. For real prompt separation, replace the reduce in `renderFromPrompt` with a full string hash such as FNV-1a or djb2.
- An empty prompt makes `charCodeAt(0)` return `NaN`, so the seed is `NaN`. ToInt32 turns that into 0 inside the XOR and the output degenerates to a pattern driven by `y` alone. Guard the input if empty strings are possible.
- `renderFromPrompt` is declared `async` but never awaits anything, so it returns an already resolved promise. On a cache hit it resolves to `undefined`, because `drawPixels` returns nothing. Do not rely on getting an `ImageData` from both paths.
- The cache is unbounded and lives for the lifetime of the instance. Every distinct prompt and size pair holds a full `ImageData`, which is `width * height * 4` bytes, roughly one megabyte at 512 by 512. Add eviction if prompts are user supplied.
- The cache hit path calls `putImageData` without resetting `canvas.width` and `canvas.height`. If anything else resized the canvas between calls, the blit will not fill it.
- Browser only. It needs a real canvas element and a 2d context, and it exports as an ES module. Node requires a canvas shim. Zero third party dependencies.
- `modelEndpoint`, `queue` and `rendering` are dead fields today. No request is ever issued, so the prompt cannot leak off device.
