#import <Foundation/Foundation.h>

NS_ASSUME_NONNULL_BEGIN

/// Error domain used for every NSError this file produces.
extern NSErrorDomain const LLMSentinelErrorDomain;

typedef NS_ENUM(NSInteger, LLMSentinelErrorCode) {
    LLMSentinelErrorBudgetExceeded = 1,
};

/// What a budget does once its threshold is crossed.
typedef NS_ENUM(NSInteger, LLMSentinelBudgetPolicy) {
    /// Let the request through. Usage is still recorded and the delegate is
    /// still told the ledger went over the line via `budgetExceededAfterRecord`.
    LLMSentinelBudgetPolicyWarn = 0,
    /// Refuse the request before it leaves the device, when the call site
    /// this file intercepted gives it a way to do that safely. See the
    /// README for exactly which call sites qualify.
    LLMSentinelBudgetPolicyBlock = 1,
};

/// Describes one LLM provider's HTTP surface: which host it lives on and
/// where the token counts live in its request/response JSON.
@interface LLMSentinelProvider : NSObject

@property (nonatomic, copy, readonly) NSString *identifier;
@property (nonatomic, strong, readonly) NSRegularExpression *hostPattern;
@property (nonatomic, copy, readonly) NSArray<NSString *> *promptTokenKeyPath;
@property (nonatomic, copy, readonly) NSArray<NSString *> *completionTokenKeyPath;
/// Model name to USD price per 1,000 prompt tokens. Update these yourself;
/// provider pricing changes on its own schedule, not this file's.
@property (nonatomic, copy, readonly) NSDictionary<NSString *, NSNumber *> *pricePerThousandPromptTokens;
@property (nonatomic, copy, readonly) NSDictionary<NSString *, NSNumber *> *pricePerThousandCompletionTokens;

/// `hostRegexPattern` is matched against `NSURLRequest.URL.host` in full
/// (`^...$` semantics are applied for you). `promptTokenKeyPath` and
/// `completionTokenKeyPath` are JSON key paths into a response body, for
/// example `@[@"usage", @"prompt_tokens"]` or `@[@"usage", @"input_tokens"]`.
- (instancetype)initWithIdentifier:(NSString *)identifier
                        hostPattern:(NSString *)hostRegexPattern
                 promptTokenKeyPath:(NSArray<NSString *> *)promptTokenKeyPath
             completionTokenKeyPath:(NSArray<NSString *> *)completionTokenKeyPath
       pricePerThousandPromptTokens:(NSDictionary<NSString *, NSNumber *> *)pricePerThousandPromptTokens
   pricePerThousandCompletionTokens:(NSDictionary<NSString *, NSNumber *> *)pricePerThousandCompletionTokens;

@end

/// A cap on tokens and/or cost for one budget key, over a rolling window.
@interface LLMSentinelBudget : NSObject

@property (nonatomic, copy, readonly) NSString *key;
/// 0 means no token cap.
@property (nonatomic, readonly) NSUInteger maxTokens;
/// 0 means no cost cap.
@property (nonatomic, readonly) double maxCostUSD;
/// 0 means lifetime cumulative, never pruned by age.
@property (nonatomic, readonly) NSTimeInterval windowSeconds;
@property (nonatomic, readonly) LLMSentinelBudgetPolicy policy;

- (instancetype)initWithKey:(NSString *)key
                   maxTokens:(NSUInteger)maxTokens
                  maxCostUSD:(double)maxCostUSD
               windowSeconds:(NSTimeInterval)windowSeconds
                      policy:(LLMSentinelBudgetPolicy)policy;

@end

/// One completed (or failed) LLM call, as this file was able to observe it.
@interface LLMSentinelUsageRecord : NSObject

@property (nonatomic, copy, readonly) NSString *requestID;
@property (nonatomic, copy, readonly) NSString *host;
@property (nonatomic, copy, readonly) NSString *providerIdentifier;
@property (nonatomic, copy, readonly, nullable) NSString *model;
@property (nonatomic, readonly) NSUInteger promptTokens;
@property (nonatomic, readonly) NSUInteger completionTokens;
/// YES when token counts came from a byte length heuristic rather than a
/// real `usage` object in the response, because none could be found.
@property (nonatomic, readonly) BOOL tokensEstimated;
@property (nonatomic, readonly) double costUSD;
@property (nonatomic, readonly) NSTimeInterval latency;
@property (nonatomic, readonly) NSInteger httpStatusCode;
@property (nonatomic, readonly) BOOL requestFailed;
/// YES when adding this record's usage pushed its budget key over its
/// configured limit. Meaningful even under `LLMSentinelBudgetPolicyWarn`,
/// where nothing was blocked but you still want to know.
@property (nonatomic, readonly) BOOL budgetExceededAfterRecord;
@property (nonatomic, copy, readonly) NSString *budgetKey;
@property (nonatomic, copy, readonly) NSDate *timestamp;
/// `request.allHTTPHeaderFields` with `Authorization`, `x-api-key` and
/// similar values replaced with the literal string `REDACTED`.
@property (nonatomic, copy, readonly) NSDictionary<NSString *, NSString *> *redactedHeaders;

@end

@class LLMNetworkCostSentinel;

@protocol LLMNetworkCostSentinelDelegate <NSObject>
@optional
- (void)sentinel:(LLMNetworkCostSentinel *)sentinel didRecordUsage:(LLMSentinelUsageRecord *)record;
- (void)sentinel:(LLMNetworkCostSentinel *)sentinel
    didBlockRequest:(NSURLRequest *)request
       forBudgetKey:(NSString *)budgetKey
             reason:(NSString *)reason;
@end

/// Process-wide observability and budget enforcement for LLM calls made
/// through `NSURLSession`, without touching the SDK or app code that makes
/// those calls. See the README for what it intercepts, what it cannot
/// intercept, and why.
@interface LLMNetworkCostSentinel : NSObject

+ (instancetype)sharedSentinel;

@property (nonatomic, weak, nullable) id<LLMNetworkCostSentinelDelegate> delegate;
@property (nonatomic, readonly, getter=isActive) BOOL active;

/// Upper bound, per task, on how much response body this file will buffer
/// for usage extraction on the delegate driven streaming path. Default is
/// 262144 (256 KiB). Does not affect what your own delegate or completion
/// handler receives, only what this file keeps a copy of.
@property (nonatomic) NSUInteger maxBufferedResponseBytesPerTask;

/// How a request is bucketed into a budget key. Defaults to
/// `request.URL.host`. Override to bucket by tenant, feature or API key
/// instead, for example by reading a custom header you attach yourself.
@property (nonatomic, copy, nullable) NSString * _Nonnull (^budgetKeyForRequest)(NSURLRequest *request);

/// Installs the runtime hooks (idempotent, safe to call once at launch) and
/// starts matching requests against `providers`. If `ledgerFileURL` is
/// non-nil, usage is appended there as newline delimited JSON and replayed
/// back into memory on the next `activateWithProviders:ledgerFileURL:` call
/// so budgets survive a relaunch.
- (void)activateWithProviders:(NSArray<LLMSentinelProvider *> *)providers
                 ledgerFileURL:(nullable NSURL *)ledgerFileURL;

/// Turns off matching and enforcement. Installed runtime hooks stay
/// installed (see the README) but become no-ops.
- (void)deactivate;

- (void)setBudget:(LLMSentinelBudget *)budget;
- (nullable LLMSentinelBudget *)budgetForKey:(NSString *)key;
- (void)removeBudgetForKey:(NSString *)key;

- (NSUInteger)consumedTokensForBudgetKey:(NSString *)key;
- (double)consumedCostUSDForBudgetKey:(NSString *)key;
- (void)resetUsageForBudgetKey:(NSString *)key;

/// Illustrative starting points, not a live price feed. Update the price
/// dictionaries to match your actual contract before trusting the totals.
+ (LLMSentinelProvider *)openAIProvider;
+ (LLMSentinelProvider *)anthropicProvider;

@end

NS_ASSUME_NONNULL_END
