# On Device Model Fetcher

A resumable, hash checked, disk aware downloader for multi gigabyte model weights on iOS and macOS, written in Objective-C. It exists because shipping a local LLM or Core ML model inside an app means downloading a file that is bigger than most apps, over networks that drop, and loading it only if every byte is right.

**Language:** Objective-C | **Lines:** 773 | **Added:** 2026-10-19

## What this solves

If you ship an on device LLM, a GGUF file, a Core ML package or any large embedding model, you cannot put it in the app bundle. App Store limits and download sizes make that a bad idea. So the app downloads the weights on first launch, and that is where most teams write a thin wrapper around `NSURLSessionDownloadTask` and move on.

That wrapper breaks in ways you only see in production:

- The user walks into a lift at 70 percent. The connection dies. The wrapper starts again from zero, on a 4 GB file.
- The server ignores the `Range` header and sends the whole file with a 200. A naive resume appends a full body onto a partial one and you get a file that is 6 GB and useless.
- The CDN replaces the file between two attempts. The first half is version A and the second half is version B. Same size, wrong bytes, and the model loads and produces garbage or crashes inside the inference runtime.
- The disk has 1.5 GB free. The download fills it, the app dies with a write error and the user's photos app now complains too.
- A proxy closes the connection cleanly before the last byte. No error is raised. The file is just short.
- A 429 or 503 comes back with a `Retry-After` and the client retries in a tight loop.

This code handles each of these on purpose. The result is a file that either matches the SHA-256 you pinned or does not exist at the final path. There is no third state where a half trusted file sits where the model loader will find it.

## Why I built it

I kept seeing the same bug report in on device AI projects: "the model sometimes fails to load after an update." Nearly every time the real cause was a bad download that nobody verified. The file existed, it had the right name and it was the wrong bytes. The loader crashed with an opaque error from deep inside a C++ runtime and the team spent a day blaming quantisation.

Background `NSURLSessionDownloadTask` with resume data is fine for small files, but resume data is opaque, it can be rejected for reasons you cannot see, and it tells you nothing about whether the finished file is the one you meant to ship. For weights you want control: your own partial file, your own digest, your own rules about when to splice and when to start over.

So this is the version I wanted to find and could not. It is one class and one spec object. It does not try to be a general download manager. It does one job: get one large file onto the device, correctly, and say clearly when it cannot.

## When to use it

Use it when:

- Your app downloads model weights, tokenizer data, vector indexes or any artifact above a few hundred megabytes that you can pin by SHA-256.
- You control or at least know the expected size and digest, for example from a signed manifest or a release record.
- You want resume across app launches, network drops and server hiccups without trusting opaque resume data.
- You want a hard failure on corruption instead of a silent bad load.

Do not use it when:

- The file is small. A plain download task is simpler and fine.
- You cannot know the digest ahead of time. This code refuses to run without one, by design.
- You need transfers to continue while the app is suspended. This uses a foreground style data task so it can stream into a hash. If you need true background transfer, download with a background session and call the same verify and publish logic yourself. That is a real limit and it is listed in the notes below.

## How it works

### The spec

`ODMFetchSpec` holds the source URL, `sha256Hex`, `expectedBytes`, a `fileName` and optional `extraHeaders`. The designated initialiser `initWithSourceURL:sha256Hex:expectedBytes:fileName:error:` validates everything up front. The URL must be https. The digest must be 64 hex characters. The size must be above zero. The file name must be a plain leaf name: no slashes, no backslashes, no leading dot, no NUL. That last check matters because a manifest you downloaded can be attacker influenced, and a `fileName` of `../../something` would otherwise write outside your directory.

### Files on disk

For a spec named `model.gguf` the fetcher keeps `model.gguf.part` for the bytes and `model.gguf.part.meta` for a small JSON sidecar. The sidecar stores the URL, the digest, the size and the validators the server gave us, the `ETag` and `Last-Modified` values. `loadMetaMatchingSpec` compares the sidecar to the current spec. If anything differs, or the sidecar is missing or torn, `discardPartialForSpec:` throws the partial away. We never splice bytes across two different artifacts.

The final file only ever appears by an atomic move from the `.part` file after the digest matched. That is why `verifiedURLForSpec:` can trust a file that exists with the right size. Nothing else ever writes to that path.

### Preflight

`beginRunForGeneration:` first checks for an already verified copy and finishes at once if it exists. Then it checks the disk. The required space is the remaining bytes plus headroom, where headroom is the larger of `diskHeadroomFloorBytes` (256 MiB by default) and `diskHeadroomFraction` of the expected size (5 percent by default). `availableBytesAtDirectory` asks for `NSURLVolumeAvailableCapacityForImportantUsageKey` first, which is the number iOS actually lets you use, and falls back to the plain capacity key. If there is not enough room you get `ODMFetcherErrorInsufficientDisk` with the exact numbers in the message, before a single byte is written.

### Resuming without trusting anything

`rehashPartAndOpenHandle` reads the existing partial file through `CC_SHA256_Update` in 1 MiB chunks (`kODMHashChunkBytes`). This costs a few seconds on a large partial, and it buys an exact running digest, so the final check does not depend on any earlier session having been honest. The progress callback reports `ODMFetchPhaseHashingPrefix` during this so your UI does not look frozen.

`startAttempt` then builds a `GET`. When there are bytes on disk it sends `Range: bytes=N-` and `If-Range` with the strong `ETag` or, failing that, the `Last-Modified` value. `If-Range` is the important part. If the object changed, a correct server answers 200 with the full body rather than a 206 slice of a new file.

### Reading the response

`URLSession:dataTask:didReceiveResponse:completionHandler:` is the heart of it:

- **200:** If we had a partial, the server ignored our range or the object changed. `resetPartialToEmpty` truncates the file and restarts the hash. If `expectedContentLength` disagrees with the spec the fetch fails with `ODMFetcherErrorSizeMismatch` and does not retry, because retrying cannot fix a wrong manifest.
- **206:** `ODMParseContentRange` parses `bytes start-end/total`. The start must equal our bytes on disk and the total must equal the spec size. Anything else goes to `handleBadRangeResponse`, which restarts from zero once. A second mismatch is a hard `ODMFetcherErrorBadRangeResponse`.
- **416:** If we already have every byte, go straight to verification. Otherwise treat it as a bad range.
- **408, 425, 429 and 5xx:** transient. Schedule a retry.
- **Other statuses:** permanent, `ODMFetcherErrorHTTPStatus`.

`adoptValidatorsFromResponse:` only keeps strong ETags, because weak ones (`W/`) are not allowed in `If-Range`.

### Streaming and the last byte

`URLSession:dataTask:didReceiveData:` writes each chunk to the file handle and into the running hash, and stops with `ODMFetcherErrorSizeMismatch` and a discarded partial if the server sends more than `expectedBytes`. Write errors, including a full disk mid transfer, become `ODMFetcherErrorFilesystem`.

When the task completes without an error but `_bytesOnDisk` is below the expected size, that is the silent truncation case. It is treated as a transient failure and retried with a range, not as success.

### Retry policy

`backoffForAttempt:base:cap:randomUnit:` is full jitter exponential backoff with a floor of a quarter of the ceiling, so two clients that fail together do not retry together and a lucky near zero draw does not hammer a struggling CDN. `retryAfterSecondsFromHeader:now:` understands both delta seconds and HTTP dates. `scheduleRetryWithReason:` takes the larger of the computed delay and the server's hint, capped by `maxBackoffSeconds`. Defaults are six attempts (`maxAttempts`), 1.5 seconds base and a 60 second cap. Both helpers are class methods that take their inputs as arguments so you can unit test them without a network or a clock.

Transport errors are classified by `isRetryableTransportError:`. Timeouts, lost connections, DNS failures and offline states retry. Things like a malformed URL do not.

### Verify and publish

`verifyAndPublish` finalises a copy of the hash context and compares it to the pinned digest. On a mismatch it deletes the partial and the sidecar and returns `ODMFetcherErrorDigestMismatch` with both digests. Corrupt bytes are never resumed. On a match it flushes with `F_FULLFSYNC` (falling back to `fsync`), moves the file into place, sets `NSURLIsExcludedFromBackupKey` so a multi gigabyte model does not end up in iCloud backups, and removes the sidecar.

### Concurrency

All state lives on one serial queue, which is also the delegate queue. A `_generation` counter is bumped on every start, cancel and finish, and delayed retry blocks check it so a cancelled run cannot restart itself. Only one fetch runs per instance. A second call to `fetchSpec:progress:completion:error:` returns NO with `ODMFetcherErrorBusy`. Callbacks are delivered on the main queue.

## Usage

```objc
#import "OnDeviceModelFetcher.h"

NSURL *support = [[NSFileManager defaultManager] URLForDirectory:NSApplicationSupportDirectory
                                                         inDomain:NSUserDomainMask
                                                appropriateForURL:nil
                                                           create:YES
                                                            error:nil];
NSURL *modelsDir = [support URLByAppendingPathComponent:@"Models" isDirectory:YES];

NSError *error = nil;
ODMFetchSpec *spec = [[ODMFetchSpec alloc]
    initWithSourceURL:[NSURL URLWithString:@"https://models.example.com/tiny-chat-q4.gguf"]
            sha256Hex:@"0000000000000000000000000000000000000000000000000000000000000000"
        expectedBytes:2147483648ull
             fileName:@"tiny-chat-q4.gguf"
                error:&error];

ODMModelFetcher *fetcher = [[ODMModelFetcher alloc] initWithDirectoryURL:modelsDir error:&error];
fetcher.allowsCellularAccess = NO;

NSURL *ready = [fetcher verifiedURLForSpec:spec];
if (ready) {
    // load the model from `ready`
} else {
    BOOL started = [fetcher fetchSpec:spec
                             progress:^(ODMFetchProgress p) {
        double fraction = (double)p.bytesOnDisk / (double)p.totalBytes;
        NSLog(@"phase %ld %.1f%% attempt %lu", (long)p.phase, fraction * 100, (unsigned long)p.attempt);
    }
                           completion:^(NSURL *finalURL, NSError *failure) {
        if (finalURL) {
            // load the model from finalURL
        } else if (failure.code == ODMFetcherErrorInsufficientDisk) {
            // ask the user to free space, the partial is kept
        } else if (failure.code != ODMFetcherErrorCancelled) {
            NSLog(@"model fetch failed: %@", failure.localizedDescription);
        }
    }
                                error:&error];
}

// Later, for example when the user leaves the screen:
// [fetcher cancel];                    keeps the partial file for next time
// [fetcher discardPartialForSpec:spec]; removes it
```

The digest above is a placeholder of zeros. Use the real SHA-256 of your file.

## Notes

- **Platform.** It uses Foundation and CommonCrypto, so it builds for iOS 13.4 and later and macOS 10.15.4 and later (for `readDataUpToLength:error:` and the throwing file handle methods). I could not compile it in the Linux environment this was written in because Foundation is not available there, so build it once in Xcode before you trust it, and run the two pure helpers under XCTest.
- **Background transfers.** It runs in an ephemeral foreground session so bytes can flow through the hash as they arrive. If iOS suspends the app, the transfer pauses and resumes from the partial file on the next run. If you need transfers that continue while suspended, use a background session and reuse the digest and publish steps.
- **The digest is the trust anchor.** The code is only as safe as where you got the SHA-256 from. Fetch it from a signed manifest or compile it in. Do not read it from the same server and path as the weights.
- **Compression.** `Accept-Encoding: identity` is set on purpose. Content encoding and byte ranges do not mix reliably and the digest is over the raw file.
- **One fetch per instance.** Create one fetcher per model if you need parallel downloads, and keep them in separate directories or with distinct file names.
- **Redirects.** Signed CDN redirects are followed by the system. The `Range` and `If-Range` headers are carried across them. If your CDN signs URLs with a short expiry, a long backoff can outlive the signature: set `maxBackoffSeconds` lower or wrap the fetch so the spec URL is refreshed on `ODMFetcherErrorHTTPStatus` 403.
- **Not covered.** Multi part downloads, delta updates between model versions and signature verification of the file itself are out of scope. The SHA-256 gives integrity against corruption and swaps, not authenticity if the digest source is untrusted.
