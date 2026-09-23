# LLM Network Cost Sentinel

Every iOS and macOS app that calls OpenAI, Anthropic or a similar API ends up with LLM cost tracking scattered across a dozen call sites, or no tracking at all, because the calls run through whatever third party SDK the app happens to use and nobody wants to fork it just to log a token count. This file hooks `NSURLSession` itself with the Objective-C runtime, so every LLM call in the process gets counted, priced and optionally blocked before it happens, without changing a single line of the code that makes the call.

**Language:** Objective-C | **Lines:** 1375 total (1218 .m, 157 .h) | **Added:** 2026-09-23

## What this solves

An app rarely makes its OpenAI or Anthropic calls through one tidy network layer. There is the hand rolled `URLSession` wrapper someone wrote in 2024, the third party Swift package that wraps the chat completions endpoint, the Whisper transcription call that uploads audio as multipart data, and the one screen where an engineer just called `URLSession.shared.data(for:)` inline because it was Tuesday. Every one of those paths spends real money, and none of them report to the same place.

The usual fix is to ask every call site to log its own usage, which works for about three months until someone adds a new call site and forgets. The other usual fix is a proxy server that all traffic routes through, which is the right answer for a backend and a large lift for a client app that just wants to know it is not about to blow through its OpenAI budget on the device.

`LLMNetworkCostSentinel` takes a third path. It swizzles `NSURLSession` at the four places every one of those call sites eventually goes through: `dataTaskWithRequest:completionHandler:`, `uploadTaskWithRequest:fromData:completionHandler:`, `sessionWithConfiguration:delegate:delegateQueue:`, and `-resume` on whatever private concrete `NSURLSessionTask` subclass the OS actually uses that release. Match a request's host against a configured LLM provider and this file starts counting prompt and completion tokens, pricing them, writing them to an append only ledger, and, for the call sites where it is actually safe to do so, refusing the request outright once a budget is spent.

It also solves the coordination problem: you configure budgets and providers once, in `LLMNetworkCostSentinel.activateWithProviders:ledgerFileURL:`, and every subsequent LLM call in the process is covered automatically, including calls made by SDKs you did not write and cannot edit.

## Why I built it

I kept seeing the same postmortem shape: an app shipped, a screen quietly looped a chat completion call, and nobody noticed until the OpenAI bill showed up three weeks later. Server side rate limiting and cost dashboards exist for exactly this reason, but a lot of AI features now run client side, calling the provider directly from the device, and there is nothing standing between a bug and a five figure bill except whatever ad hoc counter someone remembered to add.

I also wanted to understand exactly how far you can push Objective-C's runtime for this kind of cross cutting observability, because it is a genuinely different tool than Swift here. Swift does not give you `objc_allocateClassPair`, `class_addMethod` or `method_exchangeImplementations` without importing the Objective-C runtime headers itself, and even then you are fighting the language's static dispatch defaults the whole way. Objective-C's dynamic dispatch is what makes it possible to intercept `NSURLSession` traffic across the whole app without touching the code that generates it, the same trick KVO has used since Mac OS X 10.3.

## When to use it

- Your app calls an LLM API directly from iOS, iPadOS or macOS code, through any mix of hand written `URLSession` calls, an official or community SDK, or Swift's `async`/`await` networking.
- You want a hard spend or token ceiling enforced before a request leaves the device, not just a number in a dashboard after the fact.
- You are integrating a third party AI SDK you cannot modify and want visibility into what it is actually sending and costing.
- You need an audit trail of LLM calls, with credentials redacted, for cost attribution or incident review.
- You are budgeting per feature or per user tier and want that enforced centrally instead of threaded through every call site by hand.

## How it works

`LLMNetworkCostSentinel` is a singleton (`+sharedSentinel`). Calling `-activateWithProviders:ledgerFileURL:` stores the `LLMSentinelProvider` list, replays any existing ledger file at that URL back into memory so budgets survive a relaunch, and installs the runtime hooks exactly once through `-_installSwizzlesOnce`, guarded by a `_swizzlesInstalled` flag inside `@synchronized(self)`.

A `LLMSentinelProvider` is just a host pattern, a JSON key path to prompt and completion token counts in a response, and per model USD prices. `+openAIProvider` and `+anthropicProvider` return ready made ones for `api.openai.com` and `api.anthropic.com`; the prices baked into them are a starting point, not a live feed, update them to match your own contract.

Interception happens at four points. `-[NSURLSession dataTaskWithRequest:completionHandler:]` and `-[NSURLSession uploadTaskWithRequest:fromData:completionHandler:]` are swizzled directly, since `NSURLSession` itself is not a private class cluster the way its tasks are, so the original implementations live in plain static `IMP` variables (`_LLMSentinelOriginalDataTaskWithRequestCompletionHandler` and friends) rather than anything keyed by class identity. Each swizzled creation method wraps the caller's completion handler in one that reports back to the sentinel before calling through, and registers a `_LLMSentinelTaskContext` for the returned task, matched against the provider list by `-_makeContextForRequest:`.

`-resume` is different, because the object a `dataTaskWithURL:` call actually returns is an instance of a private concrete subclass of `NSURLSessionTask` that Apple does not document and has changed across OS releases. `-_discoverResumableTaskClasses` creates two short lived probe sessions, default and ephemeral configuration, makes a throwaway data task on each, and walks up the class hierarchy with `_LLMSentinelClassDirectlyImplementing` to find the exact class that defines `-resume` on its own method list rather than inheriting it. Those classes, not the nominal `NSURLSessionTask`, are what actually get swizzled, with `_LLMSentinelInstallResumeSwizzle` storing each class's original `-resume` as an associated object keyed per class, since more than one concrete class can exist. A background `NSURLSessionConfiguration` is deliberately never probed, because creating one has real side effects on the OS, so background session tasks are not intercepted.

The swizzled `-resume`, `_LLMSentinel_resume`, calls `-_interceptResumeForTask:` before doing anything else. That method builds or finds the task's context, freshly checks the relevant `LLMSentinelBudget` under the sentinel's `os_unfair_lock`, and decides whether to block. Blocking only actually happens when the context has a completion handler it captured at creation time, because that is the one case where synthesizing a failure is safe: the handler is called directly with a `LLMSentinelErrorBudgetExceeded` error and the real `-resume` is never invoked, so the request never leaves the device. A task without a captured completion handler, meaning it was created through a delegate driven or `async`/`await` path, is still counted and still reported through `sentinel:didBlockRequest:forBudgetKey:reason:`, but is allowed to proceed, because safely failing it would require handing a fabricated `NSURLSession` reference to `URLSession:task:didCompleteWithError:`, and this file will not fake that.

For requests it does not hard block, response bodies are observed one of two ways. Completion handler tasks hand back the full `NSData` body directly. Delegate driven tasks, including the private per task delegate Swift's `URLSession.data(for:)` has used internally since iOS 15 and macOS 12, are picked up by isa swizzling the delegate object itself: `_LLMSentinelDynamicSubclassFor` builds one dynamic subclass per original delegate class, KVO style, overriding `URLSession:dataTask:didReceiveResponse:completionHandler:`, `URLSession:dataTask:didReceiveData:` and `URLSession:task:didCompleteWithError:` with encodings pulled straight from the `NSURLSessionDataDelegate` and `NSURLSessionTaskDelegate` protocol definitions, then forwarding to whatever the original class implemented. `-_observeReceivedData:forTask:` buffers response bytes up to `maxBufferedResponseBytesPerTask` (256 KiB by default) so an open ended streaming response cannot grow this file's own memory usage without bound.

Once a task finishes, `-_finalizeTask:context:bodyData:error:` runs once per task. It tries `LLMSentinelParseWholeJSONObject` first for a plain JSON response, then `LLMSentinelLastUsageFrameFromSSEBuffer` for an SSE stream, scanning `data:` frames for the last one whose JSON contains a usage key path. If neither finds real numbers, `LLMSentinelEstimateTokenCount` falls back to a four bytes per token heuristic and the resulting `LLMSentinelUsageRecord` is marked `tokensEstimated = YES`. Cost is looked up from the provider's price tables by model name; an unpriced or unknown model still gets its tokens counted, just at zero cost. The entry is appended to the in memory ledger under lock, pruned to the budget's `windowSeconds` or capped at 5,000 entries per key when no budget is set, written as one line of JSON to the ledger file on a dedicated serial `dispatch_queue_t` with an `fsync` after every write, and finally handed to the delegate's `sentinel:didRecordUsage:` on the main queue.

## Usage

```objc
#import "LLMNetworkCostSentinel.h"

LLMNetworkCostSentinel *sentinel = [LLMNetworkCostSentinel sharedSentinel];
sentinel.delegate = self;

NSURL *ledgerURL = [[[NSFileManager defaultManager] URLsForDirectory:NSApplicationSupportDirectory
                                                             inDomains:NSUserDomainMask].firstObject
                     URLByAppendingPathComponent:@"llm-usage.ndjson"];

[sentinel activateWithProviders:@[[LLMNetworkCostSentinel openAIProvider],
                                   [LLMNetworkCostSentinel anthropicProvider]]
                   ledgerFileURL:ledgerURL];

// Bucket budgets per feature instead of per host.
sentinel.budgetKeyForRequest = ^NSString *(NSURLRequest *request) {
    NSString *feature = request.allHTTPHeaderFields[@"X-Feature-Name"];
    return feature ?: request.URL.host;
};

LLMSentinelBudget *dailyChatBudget = [[LLMSentinelBudget alloc] initWithKey:@"chat"
                                                                    maxTokens:0
                                                                   maxCostUSD:5.0
                                                                windowSeconds:86400
                                                                       policy:LLMSentinelBudgetPolicyBlock];
[sentinel setBudget:dailyChatBudget];

// Any dataTaskWithRequest:completionHandler: call to a configured provider
// now goes through the sentinel automatically. Nothing about this call
// changes; it is either allowed through, or fails with
// LLMSentinelErrorBudgetExceeded once the budget above is spent.
NSURLSessionDataTask *task = [[NSURLSession sharedSession] dataTaskWithRequest:chatRequest
                                                              completionHandler:^(NSData *data, NSURLResponse *response, NSError *error) {
    // handled exactly as it would be without the sentinel installed
}];
[task resume];

// Elsewhere, read current spend without waiting for a delegate callback.
double spentToday = [sentinel consumedCostUSDForBudgetKey:@"chat"];

#pragma mark - LLMNetworkCostSentinelDelegate

- (void)sentinel:(LLMNetworkCostSentinel *)sentinel didRecordUsage:(LLMSentinelUsageRecord *)record {
    NSLog(@"%@ %@ tokens=%lu+%lu cost=$%.4f estimated=%d",
          record.providerIdentifier, record.model,
          (unsigned long)record.promptTokens, (unsigned long)record.completionTokens,
          record.costUSD, record.tokensEstimated);
}

- (void)sentinel:(LLMNetworkCostSentinel *)sentinel
    didBlockRequest:(NSURLRequest *)request
       forBudgetKey:(NSString *)budgetKey
             reason:(NSString *)reason {
    NSLog(@"blocked a call for budget '%@': %@", budgetKey, reason);
}
```

## Notes

- Compile with ARC enabled. The file uses `os_unfair_lock` (iOS 10 / macOS 10.12 and later) and falls back gracefully on older systems for the per task delegate pickup, which is only reachable on iOS 15 / macOS 12 and later.
- Hard blocking (`LLMSentinelBudgetPolicyBlock`) only takes effect for requests created through `dataTaskWithRequest:completionHandler:` or `uploadTaskWithRequest:fromData:completionHandler:`. Delegate driven tasks and Swift's `URLSession.data(for:)` are still counted, priced and reported through `sentinel:didBlockRequest:forBudgetKey:reason:` as an advisory, but the network call itself is not prevented, because safely synthesizing a delegate failure callback would require a real `NSURLSession` reference this code does not have. Route budget sensitive `async`/`await` call sites through `dataTaskWithRequest:completionHandler:` wrapped in a checked continuation, or check `consumedCostUSDForBudgetKey:` yourself first, if you need a hard stop there too.
- Background `NSURLSessionConfiguration` sessions are not intercepted. Discovering their concrete task class would mean creating a real background session purely to inspect it, which registers a system level background transfer client as a side effect, and that is not a trade this file makes on your behalf.
- Request bodies sent through `HTTPBodyStream` are never read, because a body stream can only be consumed once; reading it here to find the `model` field would send an empty body to the real network stack. Those requests are still matched by host and still budgeted, just without a model name or a request side token estimate.
- `maxBufferedResponseBytesPerTask` only bounds memory this file accumulates on the delegate driven streaming path. `dataTaskWithRequest:completionHandler:` responses are already fully materialized by the OS before your handler runs, independent of anything here.
- Header and API key values in `LLMSentinelUsageRecord.redactedHeaders` are fully replaced with the literal string `REDACTED`, not partially shown, for `Authorization`, `x-api-key`, `api-key`, `Proxy-Authorization`, `x-goog-api-key` and `x-auth-token`, case insensitively.
- `deactivate` stops enforcement and observation but does not physically remove the installed swizzles, which is the standard, safe way to work with runtime patched methods. Call `activate`/`deactivate` from one place at startup rather than toggling them from multiple threads.
- The provider price tables in `+openAIProvider` and `+anthropicProvider` are illustrative starting points current as of when this file was written, not a live price feed. Provider pricing changes on its own schedule; update the dictionaries to match your actual contract before trusting the cost totals.
- Budget enforcement happens only before a request starts, checked fresh at the moment `-resume` runs. Several requests resumed at nearly the same instant on different threads can each see the budget as not yet exceeded and all proceed, since checking and reserving are not a single atomic step across a call already under way. For a hard ceiling across many tenants that cannot be raced, enforce it server side as well.
