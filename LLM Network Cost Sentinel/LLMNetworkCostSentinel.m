#import "LLMNetworkCostSentinel.h"

#import <fcntl.h>
#import <math.h>
#import <objc/message.h>
#import <objc/runtime.h>
#import <os/lock.h>
#import <unistd.h>

NSErrorDomain const LLMSentinelErrorDomain = @"LLMNetworkCostSentinel";

static const NSUInteger kLLMSentinelDefaultMaxBufferedBytes = 262144;
static const NSUInteger kLLMSentinelMaxLedgerEntriesWithoutBudget = 5000;

#pragma mark - Associated object keys

static const void *kLLMSentinelOrigDidReceiveResponse = &kLLMSentinelOrigDidReceiveResponse;
static const void *kLLMSentinelOrigDidReceiveData = &kLLMSentinelOrigDidReceiveData;
static const void *kLLMSentinelOrigDidComplete = &kLLMSentinelOrigDidComplete;
static const void *kLLMSentinelOrigResume = &kLLMSentinelOrigResume;
static const void *kLLMSentinelWrappedFlag = &kLLMSentinelWrappedFlag;

// `NSURLSession` itself (unlike its task classes) is not a private class
// cluster, so the three methods below are swizzled exactly once, always on
// the same class, and the original implementations are kept in plain
// statics rather than associated objects: there is no per-instance or
// per-subclass identity ambiguity to resolve at call time here.
static IMP _LLMSentinelOriginalDataTaskWithRequestCompletionHandler = NULL;
static IMP _LLMSentinelOriginalUploadTaskWithRequestFromDataCompletionHandler = NULL;
static IMP _LLMSentinelOriginalSessionWithConfigurationDelegateDelegateQueue = NULL;

#pragma mark - LLMSentinelProvider

@implementation LLMSentinelProvider

- (instancetype)initWithIdentifier:(NSString *)identifier
                        hostPattern:(NSString *)hostRegexPattern
                 promptTokenKeyPath:(NSArray<NSString *> *)promptTokenKeyPath
             completionTokenKeyPath:(NSArray<NSString *> *)completionTokenKeyPath
       pricePerThousandPromptTokens:(NSDictionary<NSString *, NSNumber *> *)pricePerThousandPromptTokens
   pricePerThousandCompletionTokens:(NSDictionary<NSString *, NSNumber *> *)pricePerThousandCompletionTokens {
    self = [super init];
    if (self) {
        _identifier = [identifier copy];
        NSString *anchored = [NSString stringWithFormat:@"^(?:%@)$", hostRegexPattern];
        NSError *regexError = nil;
        _hostPattern = [NSRegularExpression regularExpressionWithPattern:anchored
                                                                   options:NSRegularExpressionCaseInsensitive
                                                                     error:&regexError];
        NSAssert(_hostPattern != nil, @"LLMSentinelProvider: invalid host pattern '%@': %@", hostRegexPattern, regexError);
        _promptTokenKeyPath = [promptTokenKeyPath copy];
        _completionTokenKeyPath = [completionTokenKeyPath copy];
        _pricePerThousandPromptTokens = [pricePerThousandPromptTokens copy];
        _pricePerThousandCompletionTokens = [pricePerThousandCompletionTokens copy];
    }
    return self;
}

@end

#pragma mark - LLMSentinelBudget

@implementation LLMSentinelBudget

- (instancetype)initWithKey:(NSString *)key
                   maxTokens:(NSUInteger)maxTokens
                  maxCostUSD:(double)maxCostUSD
               windowSeconds:(NSTimeInterval)windowSeconds
                      policy:(LLMSentinelBudgetPolicy)policy {
    self = [super init];
    if (self) {
        _key = [key copy];
        _maxTokens = maxTokens;
        _maxCostUSD = maxCostUSD;
        _windowSeconds = windowSeconds;
        _policy = policy;
    }
    return self;
}

@end

#pragma mark - LLMSentinelUsageRecord

@interface LLMSentinelUsageRecord ()
- (instancetype)initWithRequestID:(NSString *)requestID
                              host:(NSString *)host
                providerIdentifier:(NSString *)providerIdentifier
                             model:(nullable NSString *)model
                      promptTokens:(NSUInteger)promptTokens
                  completionTokens:(NSUInteger)completionTokens
                   tokensEstimated:(BOOL)tokensEstimated
                           costUSD:(double)costUSD
                           latency:(NSTimeInterval)latency
                    httpStatusCode:(NSInteger)httpStatusCode
                     requestFailed:(BOOL)requestFailed
        budgetExceededAfterRecord:(BOOL)budgetExceededAfterRecord
                         budgetKey:(NSString *)budgetKey
                   redactedHeaders:(NSDictionary<NSString *, NSString *> *)redactedHeaders;
@end

@implementation LLMSentinelUsageRecord

- (instancetype)initWithRequestID:(NSString *)requestID
                              host:(NSString *)host
                providerIdentifier:(NSString *)providerIdentifier
                             model:(nullable NSString *)model
                      promptTokens:(NSUInteger)promptTokens
                  completionTokens:(NSUInteger)completionTokens
                   tokensEstimated:(BOOL)tokensEstimated
                           costUSD:(double)costUSD
                           latency:(NSTimeInterval)latency
                    httpStatusCode:(NSInteger)httpStatusCode
                     requestFailed:(BOOL)requestFailed
        budgetExceededAfterRecord:(BOOL)budgetExceededAfterRecord
                         budgetKey:(NSString *)budgetKey
                   redactedHeaders:(NSDictionary<NSString *, NSString *> *)redactedHeaders {
    self = [super init];
    if (self) {
        _requestID = [requestID copy];
        _host = [host copy];
        _providerIdentifier = [providerIdentifier copy];
        _model = [model copy];
        _promptTokens = promptTokens;
        _completionTokens = completionTokens;
        _tokensEstimated = tokensEstimated;
        _costUSD = costUSD;
        _latency = latency;
        _httpStatusCode = httpStatusCode;
        _requestFailed = requestFailed;
        _budgetExceededAfterRecord = budgetExceededAfterRecord;
        _budgetKey = [budgetKey copy];
        _timestamp = [NSDate date];
        _redactedHeaders = [redactedHeaders copy];
    }
    return self;
}

@end

#pragma mark - Private task context

@interface _LLMSentinelTaskContext : NSObject
@property (nonatomic, strong) LLMSentinelProvider *provider;
@property (nonatomic, copy) NSString *budgetKey;
@property (nonatomic, copy, nullable) NSString *model;
@property (nonatomic, strong) NSMutableData *responseBuffer;
@property (nonatomic) BOOL bufferTruncated;
@property (nonatomic) BOOL bodyWasStreamed;
@property (nonatomic) NSUInteger requestByteLength;
@property (nonatomic, strong, nullable) NSDate *startDate;
@property (nonatomic, copy, nullable) void (^completionHandler)(NSData *_Nullable, NSURLResponse *_Nullable, NSError *_Nullable);
@property (nonatomic) NSInteger httpStatusCode;
@property (nonatomic, copy) NSDictionary<NSString *, NSString *> *redactedHeaders;
@property (nonatomic) BOOL finalized;
@end

@implementation _LLMSentinelTaskContext
- (instancetype)init {
    self = [super init];
    if (self) {
        _httpStatusCode = -1;
        _responseBuffer = [NSMutableData data];
        _redactedHeaders = @{};
    }
    return self;
}
@end

#pragma mark - Free functions: JSON, SSE, redaction, estimation

static NSString *_Nullable LLMSentinelModelFromRequestBody(NSData *_Nullable body) {
    if (body.length == 0) {
        return nil;
    }
    id json = [NSJSONSerialization JSONObjectWithData:body options:0 error:NULL];
    if (![json isKindOfClass:[NSDictionary class]]) {
        return nil;
    }
    id model = ((NSDictionary *)json)[@"model"];
    return [model isKindOfClass:[NSString class]] ? model : nil;
}

static NSNumber *_Nullable LLMSentinelValueAtKeyPath(id _Nullable json, NSArray<NSString *> *keyPath) {
    id current = json;
    for (NSString *key in keyPath) {
        if (![current isKindOfClass:[NSDictionary class]]) {
            return nil;
        }
        current = ((NSDictionary *)current)[key];
    }
    return [current isKindOfClass:[NSNumber class]] ? current : nil;
}

/// Non streaming case: the whole response body is one JSON document.
static NSDictionary *_Nullable LLMSentinelParseWholeJSONObject(NSData *body) {
    id json = [NSJSONSerialization JSONObjectWithData:body options:0 error:NULL];
    return [json isKindOfClass:[NSDictionary class]] ? json : nil;
}

/// Streaming case: the body is a sequence of `data: {...}` SSE frames.
/// Returns the last frame whose JSON contains either key path, since
/// providers emit the usage object once, at or near the end of the stream.
static NSDictionary *_Nullable LLMSentinelLastUsageFrameFromSSEBuffer(NSData *buffer,
                                                                       NSArray<NSString *> *promptPath,
                                                                       NSArray<NSString *> *completionPath) {
    NSString *text = [[NSString alloc] initWithData:buffer encoding:NSUTF8StringEncoding];
    if (text.length == 0) {
        return nil;
    }
    NSDictionary *lastMatch = nil;
    NSCharacterSet *whitespace = [NSCharacterSet whitespaceAndNewlineCharacterSet];
    for (NSString *rawLine in [text componentsSeparatedByString:@"\n"]) {
        NSString *line = [rawLine stringByTrimmingCharactersInSet:whitespace];
        if (![line hasPrefix:@"data:"]) {
            continue;
        }
        NSString *payload = [[line substringFromIndex:5] stringByTrimmingCharactersInSet:whitespace];
        if (payload.length == 0 || [payload isEqualToString:@"[DONE]"]) {
            continue;
        }
        NSData *payloadData = [payload dataUsingEncoding:NSUTF8StringEncoding];
        if (!payloadData) {
            continue;
        }
        id frame = [NSJSONSerialization JSONObjectWithData:payloadData options:0 error:NULL];
        if (![frame isKindOfClass:[NSDictionary class]]) {
            continue;
        }
        if (LLMSentinelValueAtKeyPath(frame, promptPath) || LLMSentinelValueAtKeyPath(frame, completionPath)) {
            lastMatch = frame;
        }
    }
    return lastMatch;
}

/// Roughly four bytes of UTF-8 text per token. This is only reached when no
/// real usage object could be found anywhere in the buffered body, and the
/// resulting record is marked `tokensEstimated = YES` so callers can tell.
static NSUInteger LLMSentinelEstimateTokenCount(NSUInteger byteLength) {
    return (NSUInteger)llround(byteLength / 4.0);
}

static NSDictionary<NSString *, NSString *> *LLMSentinelRedactedHeaders(NSDictionary<NSString *, NSString *> *_Nullable headers) {
    if (headers.count == 0) {
        return @{};
    }
    static NSSet<NSString *> *sensitive;
    static dispatch_once_t onceToken;
    dispatch_once(&onceToken, ^{
        sensitive = [NSSet setWithObjects:@"authorization", @"x-api-key", @"api-key",
                                           @"proxy-authorization", @"x-goog-api-key", @"x-auth-token", nil];
    });
    NSMutableDictionary<NSString *, NSString *> *redacted = [NSMutableDictionary dictionaryWithCapacity:headers.count];
    [headers enumerateKeysAndObjectsUsingBlock:^(NSString *key, NSString *value, BOOL *stop) {
        redacted[key] = [sensitive containsObject:key.lowercaseString] ? @"REDACTED" : value;
    }];
    return redacted;
}

#pragma mark - Runtime helpers

/// Walks up from `start` and returns the first class in the chain whose own
/// method list (not an inherited one) defines `selector`. `NSURLSessionTask`
/// itself is usually abstract; the concrete class actually implementing
/// `-resume` is a private subclass that can differ by OS version and by
/// session configuration, so this is resolved at runtime instead of assumed.
static Class _LLMSentinelClassDirectlyImplementing(Class start, SEL selector) {
    Class current = start;
    while (current) {
        unsigned int count = 0;
        Method *methods = class_copyMethodList(current, &count);
        BOOL found = NO;
        for (unsigned int i = 0; i < count; i++) {
            if (method_getName(methods[i]) == selector) {
                found = YES;
                break;
            }
        }
        if (methods) {
            free(methods);
        }
        if (found) {
            return current;
        }
        current = class_getSuperclass(current);
    }
    return Nil;
}

static const char *_LLMSentinelProtocolMethodEncoding(Protocol *protocol, SEL selector) {
    struct objc_method_description description = protocol_getMethodDescription(protocol, selector, NO, YES);
    if (description.types != NULL) {
        return description.types;
    }
    description = protocol_getMethodDescription(protocol, selector, YES, YES);
    return description.types;
}

static void _LLMSentinel_didReceiveResponse(id self, SEL _cmd, NSURLSession *session, NSURLSessionDataTask *task,
                                             NSURLResponse *response, void (^completionHandler)(NSURLSessionResponseDisposition));
static void _LLMSentinel_didReceiveData(id self, SEL _cmd, NSURLSession *session, NSURLSessionDataTask *task, NSData *data);
static void _LLMSentinel_didComplete(id self, SEL _cmd, NSURLSession *session, NSURLSessionTask *task, NSError *_Nullable error);
static void _LLMSentinel_resume(id self, SEL _cmd);

// Forward declarations for the swizzle installers and the `NSURLSession`
// level swizzled implementations, both defined near the end of this file
// but invoked from `-_installSwizzlesOnce` further up.
static void _LLMSentinelSwizzleInstanceSelector(Class cls, SEL selector, IMP newIMP, IMP *outOriginal);
static void _LLMSentinelSwizzleClassSelector(Class cls, SEL selector, IMP newIMP, IMP *outOriginal);
static void _LLMSentinelInstallResumeSwizzle(Class cls);
static NSURLSessionDataTask *_LLMSentinel_dataTaskWithRequestCompletionHandler(
    id self, SEL _cmd, NSURLRequest *request,
    void (^completionHandler)(NSData *_Nullable, NSURLResponse *_Nullable, NSError *_Nullable));
static NSURLSessionUploadTask *_LLMSentinel_uploadTaskWithRequestFromDataCompletionHandler(
    id self, SEL _cmd, NSURLRequest *request, NSData *_Nullable bodyData,
    void (^completionHandler)(NSData *_Nullable, NSURLResponse *_Nullable, NSError *_Nullable));
static NSURLSession *_LLMSentinel_sessionWithConfigurationDelegateDelegateQueue(id self, SEL _cmd,
                                                                                  NSURLSessionConfiguration *configuration,
                                                                                  id delegate, NSOperationQueue *queue);

/// Creates (once per original class, cached) a KVO style dynamic subclass
/// that overrides the three `NSURLSessionDataDelegate` / `NSURLSessionTaskDelegate`
/// methods this file needs to observe, forwarding to whatever the real
/// delegate class already implemented, or synthesizing the correct default
/// behavior when it implemented nothing for that selector.
static os_unfair_lock kLLMSentinelSubclassLock = OS_UNFAIR_LOCK_INIT;

static Class _LLMSentinelDynamicSubclassFor(Class originalClass) {
    static NSMapTable<Class, Class> *cache;
    static dispatch_once_t onceToken;
    dispatch_once(&onceToken, ^{
        cache = [NSMapTable strongToStrongObjectsMapTable];
    });

    // `objc_allocateClassPair`/`objc_registerClassPair` and the cache below
    // are not safe against two threads discovering the same never-before-seen
    // delegate class at once (two apps creating sessions on different
    // threads at startup is a completely ordinary way to hit this), so the
    // whole lookup-or-create sequence is serialized behind one lock, not just
    // the map access.
    os_unfair_lock_lock(&kLLMSentinelSubclassLock);

    Class cached = [cache objectForKey:originalClass];
    if (cached) {
        os_unfair_lock_unlock(&kLLMSentinelSubclassLock);
        return cached;
    }

    NSString *name = [NSString stringWithFormat:@"LLMSentinel_%@", NSStringFromClass(originalClass)];
    Class existing = objc_getClass(name.UTF8String);
    if (existing) {
        [cache setObject:existing forKey:originalClass];
        os_unfair_lock_unlock(&kLLMSentinelSubclassLock);
        return existing;
    }

    Class subclass = objc_allocateClassPair(originalClass, name.UTF8String, 0);
    if (!subclass) {
        // Allocation failed (name collision from outside this file, most
        // likely). Fail safe: do not wrap, let the delegate run unobserved.
        os_unfair_lock_unlock(&kLLMSentinelSubclassLock);
        return originalClass;
    }

    struct {
        SEL selector;
        IMP imp;
        Protocol *protocol;
        const void *originalKey;
    } overrides[3] = {
        {@selector(URLSession:dataTask:didReceiveResponse:completionHandler:), (IMP)_LLMSentinel_didReceiveResponse,
         @protocol(NSURLSessionDataDelegate), kLLMSentinelOrigDidReceiveResponse},
        {@selector(URLSession:dataTask:didReceiveData:), (IMP)_LLMSentinel_didReceiveData,
         @protocol(NSURLSessionDataDelegate), kLLMSentinelOrigDidReceiveData},
        {@selector(URLSession:task:didCompleteWithError:), (IMP)_LLMSentinel_didComplete,
         @protocol(NSURLSessionTaskDelegate), kLLMSentinelOrigDidComplete},
    };

    for (int i = 0; i < 3; i++) {
        SEL selector = overrides[i].selector;
        const char *types = _LLMSentinelProtocolMethodEncoding(overrides[i].protocol, selector);
        if (!types) {
            continue;
        }
        Method originalMethod = class_getInstanceMethod(originalClass, selector);
        IMP originalIMP = originalMethod ? method_getImplementation(originalMethod) : NULL;
        if (originalIMP) {
            objc_setAssociatedObject(subclass, overrides[i].originalKey, [NSValue valueWithPointer:originalIMP], OBJC_ASSOCIATION_RETAIN);
        }
        class_addMethod(subclass, selector, overrides[i].imp, types);
    }

    objc_registerClassPair(subclass);
    [cache setObject:subclass forKey:originalClass];
    os_unfair_lock_unlock(&kLLMSentinelSubclassLock);
    return subclass;
}

#pragma mark - LLMNetworkCostSentinel

@interface LLMNetworkCostSentinel ()
@property (nonatomic, strong) NSArray<LLMSentinelProvider *> *providers;
@property (nonatomic, strong) NSMutableDictionary<NSString *, LLMSentinelBudget *> *budgets;
/// budgetKey -> array of @[timestamp NSNumber, tokens NSNumber, cost NSNumber]
@property (nonatomic, strong) NSMutableDictionary<NSString *, NSMutableArray<NSArray *> *> *usageLedger;
@property (nonatomic, strong) NSMapTable<NSURLSessionTask *, _LLMSentinelTaskContext *> *taskContexts;
@property (nonatomic) int ledgerFileDescriptor;
@property (nonatomic, strong) dispatch_queue_t ioQueue;
@property (nonatomic) os_unfair_lock lock;
@end

@implementation LLMNetworkCostSentinel {
    BOOL _swizzlesInstalled;
}

@synthesize active = _active;

+ (instancetype)sharedSentinel {
    static LLMNetworkCostSentinel *instance;
    static dispatch_once_t onceToken;
    dispatch_once(&onceToken, ^{
        instance = [[self alloc] init];
    });
    return instance;
}

- (instancetype)init {
    self = [super init];
    if (self) {
        _providers = @[];
        _budgets = [NSMutableDictionary dictionary];
        _usageLedger = [NSMutableDictionary dictionary];
        _taskContexts = [NSMapTable mapTableWithKeyOptions:NSPointerFunctionsWeakMemory
                                               valueOptions:NSPointerFunctionsStrongMemory];
        _maxBufferedResponseBytesPerTask = kLLMSentinelDefaultMaxBufferedBytes;
        _ledgerFileDescriptor = -1;
        _ioQueue = dispatch_queue_create("com.llmsentinel.ledger-io", DISPATCH_QUEUE_SERIAL);
        _lock = OS_UNFAIR_LOCK_INIT;
    }
    return self;
}

#pragma mark Activation

- (void)activateWithProviders:(NSArray<LLMSentinelProvider *> *)providers ledgerFileURL:(nullable NSURL *)ledgerFileURL {
    os_unfair_lock_lock(&_lock);
    self.providers = [providers copy];
    _active = YES;
    os_unfair_lock_unlock(&_lock);

    if (ledgerFileURL) {
        [self _openLedgerFileAndReplay:ledgerFileURL];
    }

    [self _installSwizzlesOnce];
}

- (void)deactivate {
    os_unfair_lock_lock(&_lock);
    _active = NO;
    os_unfair_lock_unlock(&_lock);
}

- (void)_installSwizzlesOnce {
    @synchronized (self) {
        if (_swizzlesInstalled) {
            return;
        }
        _swizzlesInstalled = YES;

        Class sessionClass = [NSURLSession class];

        _LLMSentinelSwizzleInstanceSelector(sessionClass, @selector(dataTaskWithRequest:completionHandler:),
                                             (IMP)_LLMSentinel_dataTaskWithRequestCompletionHandler,
                                             &_LLMSentinelOriginalDataTaskWithRequestCompletionHandler);
        _LLMSentinelSwizzleInstanceSelector(sessionClass, @selector(uploadTaskWithRequest:fromData:completionHandler:),
                                             (IMP)_LLMSentinel_uploadTaskWithRequestFromDataCompletionHandler,
                                             &_LLMSentinelOriginalUploadTaskWithRequestFromDataCompletionHandler);
        _LLMSentinelSwizzleClassSelector(sessionClass, @selector(sessionWithConfiguration:delegate:delegateQueue:),
                                          (IMP)_LLMSentinel_sessionWithConfigurationDelegateDelegateQueue,
                                          &_LLMSentinelOriginalSessionWithConfigurationDelegateDelegateQueue);

        for (Class taskClass in [self _discoverResumableTaskClasses]) {
            _LLMSentinelInstallResumeSwizzle(taskClass);
        }
    }
}

/// Probes only foreground `default` and `ephemeral` configurations. A
/// background `NSURLSessionConfiguration` is deliberately never probed here:
/// creating one has real side effects (it registers a system level session
/// that can outlive the process and shows up to the OS as a background
/// transfer client), and doing that purely to read off a task's runtime
/// class would be an abuse of the API for a diagnostic that does not need
/// it. If your LLM traffic runs over a background session, its tasks are
/// not intercepted; see the README.
- (NSArray<Class> *)_discoverResumableTaskClasses {
    NSMutableSet<Class> *classes = [NSMutableSet set];
    NSArray<NSURLSessionConfiguration *> *configurations = @[
        [NSURLSessionConfiguration defaultSessionConfiguration],
        [NSURLSessionConfiguration ephemeralSessionConfiguration],
    ];
    for (NSURLSessionConfiguration *configuration in configurations) {
        NSURLSession *probeSession = [NSURLSession sessionWithConfiguration:configuration];
        NSURLSessionDataTask *probeTask = [probeSession dataTaskWithURL:[NSURL URLWithString:@"https://127.0.0.1/"]];
        Class definingClass = _LLMSentinelClassDirectlyImplementing([probeTask class], @selector(resume));
        if (definingClass) {
            [classes addObject:definingClass];
        }
        [probeTask cancel];
        [probeSession invalidateAndCancel];
    }
    return classes.allObjects;
}

#pragma mark Budgets

- (void)setBudget:(LLMSentinelBudget *)budget {
    os_unfair_lock_lock(&_lock);
    self.budgets[budget.key] = budget;
    os_unfair_lock_unlock(&_lock);
}

- (nullable LLMSentinelBudget *)budgetForKey:(NSString *)key {
    os_unfair_lock_lock(&_lock);
    LLMSentinelBudget *budget = self.budgets[key];
    os_unfair_lock_unlock(&_lock);
    return budget;
}

- (void)removeBudgetForKey:(NSString *)key {
    os_unfair_lock_lock(&_lock);
    [self.budgets removeObjectForKey:key];
    os_unfair_lock_unlock(&_lock);
}

- (NSUInteger)consumedTokensForBudgetKey:(NSString *)key {
    __block NSUInteger tokens = 0;
    os_unfair_lock_lock(&_lock);
    [self _locked_summarizeUsageForKey:key tokens:&tokens cost:NULL];
    os_unfair_lock_unlock(&_lock);
    return tokens;
}

- (double)consumedCostUSDForBudgetKey:(NSString *)key {
    __block double cost = 0;
    os_unfair_lock_lock(&_lock);
    [self _locked_summarizeUsageForKey:key tokens:NULL cost:&cost];
    os_unfair_lock_unlock(&_lock);
    return cost;
}

- (void)resetUsageForBudgetKey:(NSString *)key {
    os_unfair_lock_lock(&_lock);
    [self.usageLedger removeObjectForKey:key];
    os_unfair_lock_unlock(&_lock);
}

/// Caller must hold `_lock`. Sums entries within the key's configured
/// window (or all of them, if the budget has no window or no budget is set).
- (void)_locked_summarizeUsageForKey:(NSString *)key tokens:(NSUInteger *)outTokens cost:(double *)outCost {
    NSMutableArray<NSArray *> *entries = self.usageLedger[key];
    if (!entries) {
        return;
    }
    LLMSentinelBudget *budget = self.budgets[key];
    NSTimeInterval window = budget.windowSeconds;
    NSTimeInterval now = [NSDate date].timeIntervalSince1970;
    NSUInteger tokens = 0;
    double cost = 0;
    for (NSArray *entry in entries) {
        NSTimeInterval timestamp = [entry[0] doubleValue];
        if (window > 0 && (now - timestamp) > window) {
            continue;
        }
        tokens += [entry[1] unsignedIntegerValue];
        cost += [entry[2] doubleValue];
    }
    if (outTokens) {
        *outTokens = tokens;
    }
    if (outCost) {
        *outCost = cost;
    }
}

/// Caller must hold `_lock`.
- (BOOL)_locked_isOverBudgetForKey:(NSString *)key budget:(LLMSentinelBudget *)budget reason:(NSString **)outReason {
    NSUInteger tokens = 0;
    double cost = 0;
    [self _locked_summarizeUsageForKey:key tokens:&tokens cost:&cost];
    if (budget.maxTokens > 0 && tokens >= budget.maxTokens) {
        if (outReason) {
            *outReason = [NSString stringWithFormat:@"token budget exceeded for '%@': %lu >= %lu in the last %.0fs",
                                                      key, (unsigned long)tokens, (unsigned long)budget.maxTokens, budget.windowSeconds];
        }
        return YES;
    }
    if (budget.maxCostUSD > 0 && cost >= budget.maxCostUSD) {
        if (outReason) {
            *outReason = [NSString stringWithFormat:@"cost budget exceeded for '%@': $%.4f >= $%.4f in the last %.0fs",
                                                      key, cost, budget.maxCostUSD, budget.windowSeconds];
        }
        return YES;
    }
    return NO;
}

- (void)_locked_appendLedgerEntryForKey:(NSString *)key timestamp:(NSTimeInterval)timestamp tokens:(NSUInteger)tokens cost:(double)cost {
    NSMutableArray<NSArray *> *entries = self.usageLedger[key];
    if (!entries) {
        entries = [NSMutableArray array];
        self.usageLedger[key] = entries;
    }
    [entries addObject:@[@(timestamp), @(tokens), @(cost)]];

    LLMSentinelBudget *budget = self.budgets[key];
    if (budget.windowSeconds > 0) {
        NSTimeInterval cutoff = timestamp - budget.windowSeconds;
        NSIndexSet *stale = [entries indexesOfObjectsPassingTest:^BOOL(NSArray *entry, NSUInteger idx, BOOL *stop) {
            return [entry[0] doubleValue] < cutoff;
        }];
        if (stale.count > 0) {
            [entries removeObjectsAtIndexes:stale];
        }
    } else if (!budget && entries.count > kLLMSentinelMaxLedgerEntriesWithoutBudget) {
        [entries removeObjectsInRange:NSMakeRange(0, entries.count - kLLMSentinelMaxLedgerEntriesWithoutBudget)];
    }
}

#pragma mark Context construction and matching

- (nullable _LLMSentinelTaskContext *)_makeContextForRequest:(NSURLRequest *)request {
    NSString *host = request.URL.host;
    if (host.length == 0) {
        return nil;
    }
    LLMSentinelProvider *matched = nil;
    for (LLMSentinelProvider *provider in self.providers) {
        NSRange fullRange = NSMakeRange(0, host.length);
        if ([provider.hostPattern numberOfMatchesInString:host options:0 range:fullRange] > 0) {
            matched = provider;
            break;
        }
    }
    if (!matched) {
        return nil;
    }

    _LLMSentinelTaskContext *context = [_LLMSentinelTaskContext new];
    context.provider = matched;
    NSString *(^keyBlock)(NSURLRequest *) = self.budgetKeyForRequest;
    context.budgetKey = keyBlock ? keyBlock(request) : host;
    context.redactedHeaders = LLMSentinelRedactedHeaders(request.allHTTPHeaderFields);

    if (request.HTTPBody.length > 0) {
        context.model = LLMSentinelModelFromRequestBody(request.HTTPBody);
        context.requestByteLength = request.HTTPBody.length;
    } else if (request.HTTPBodyStream) {
        // Reading a body stream to inspect it would consume it, and it can
        // only be read once: doing so here would send an empty body to the
        // real network stack. So streamed bodies are never inspected, ever.
        context.bodyWasStreamed = YES;
    }
    return context;
}

- (nullable _LLMSentinelTaskContext *)_contextForTask:(NSURLSessionTask *)task creatingFromRequest:(nullable NSURLRequest *)request {
    os_unfair_lock_lock(&_lock);
    _LLMSentinelTaskContext *context = [self.taskContexts objectForKey:task];
    if (!context && request) {
        context = [self _makeContextForRequest:request];
        if (context) {
            [self.taskContexts setObject:context forKey:task];
        }
    }
    os_unfair_lock_unlock(&_lock);
    return context;
}

#pragma mark Observation callbacks (delegate driven path)

- (nullable _LLMSentinelTaskContext *)_existingContextForTask:(NSURLSessionTask *)task {
    os_unfair_lock_lock(&_lock);
    _LLMSentinelTaskContext *context = [self.taskContexts objectForKey:task];
    os_unfair_lock_unlock(&_lock);
    return context;
}

- (void)_observeResponse:(NSURLResponse *)response forTask:(NSURLSessionTask *)task {
    _LLMSentinelTaskContext *context = [self _existingContextForTask:task];
    if (!context) {
        return;
    }
    if ([response isKindOfClass:[NSHTTPURLResponse class]]) {
        context.httpStatusCode = ((NSHTTPURLResponse *)response).statusCode;
    }
}

- (void)_observeReceivedData:(NSData *)data forTask:(NSURLSessionTask *)task {
    _LLMSentinelTaskContext *context = [self _existingContextForTask:task];
    if (!context || context.bufferTruncated) {
        return;
    }
    NSUInteger cap = self.maxBufferedResponseBytesPerTask;
    NSUInteger currentLength = context.responseBuffer.length;
    if (currentLength >= cap) {
        context.bufferTruncated = YES;
        return;
    }
    NSUInteger room = cap - currentLength;
    if (data.length <= room) {
        [context.responseBuffer appendData:data];
    } else {
        [context.responseBuffer appendData:[data subdataWithRange:NSMakeRange(0, room)]];
        context.bufferTruncated = YES;
    }
}

- (void)_observeTaskDidComplete:(NSURLSessionTask *)task error:(nullable NSError *)error {
    _LLMSentinelTaskContext *context = [self _existingContextForTask:task];
    if (!context) {
        return;
    }
    [self _finalizeTask:task context:context bodyData:context.responseBuffer error:error];
}

#pragma mark Observation (completion handler driven path)

- (void)_handleCompletionForTask:(NSURLSessionTask *)task
                             data:(nullable NSData *)data
                         response:(nullable NSURLResponse *)response
                            error:(nullable NSError *)error {
    _LLMSentinelTaskContext *context = [self _existingContextForTask:task];
    if (!context) {
        return;
    }
    if ([response isKindOfClass:[NSHTTPURLResponse class]]) {
        context.httpStatusCode = ((NSHTTPURLResponse *)response).statusCode;
    }
    // The OS already materializes the whole body in memory before calling a
    // completion handler; this file's own buffer cap only exists to bound
    // memory it accumulates itself on the delegate driven streaming path, so
    // it is not applied again here. Scanning `data` for the trailing usage
    // object is bounded work regardless of body size.
    [self _finalizeTask:task context:context bodyData:data error:error];
}

#pragma mark Shared finalize path

- (void)_finalizeTask:(NSURLSessionTask *)task
               context:(_LLMSentinelTaskContext *)context
              bodyData:(nullable NSData *)bodyData
                 error:(nullable NSError *)error {
    if (context.finalized) {
        return;
    }
    context.finalized = YES;

    os_unfair_lock_lock(&_lock);
    [self.taskContexts removeObjectForKey:task];
    os_unfair_lock_unlock(&_lock);

    NSString *host = task.originalRequest.URL.host ?: task.currentRequest.URL.host ?: @"";
    LLMSentinelProvider *provider = context.provider;
    NSTimeInterval latency = context.startDate ? [[NSDate date] timeIntervalSinceDate:context.startDate] : 0;
    BOOL failed = (error != nil);

    NSUInteger promptTokens = 0;
    NSUInteger completionTokens = 0;
    BOOL estimated = NO;

    if (!failed && bodyData.length > 0 && provider) {
        NSDictionary *usageObject = LLMSentinelParseWholeJSONObject(bodyData);
        if (!usageObject) {
            usageObject = LLMSentinelLastUsageFrameFromSSEBuffer(bodyData, provider.promptTokenKeyPath, provider.completionTokenKeyPath);
        }
        NSNumber *promptValue = LLMSentinelValueAtKeyPath(usageObject, provider.promptTokenKeyPath);
        NSNumber *completionValue = LLMSentinelValueAtKeyPath(usageObject, provider.completionTokenKeyPath);
        if (promptValue || completionValue) {
            promptTokens = promptValue.unsignedIntegerValue;
            completionTokens = completionValue.unsignedIntegerValue;
        } else {
            estimated = YES;
            promptTokens = context.bodyWasStreamed ? 0 : LLMSentinelEstimateTokenCount(context.requestByteLength);
            completionTokens = LLMSentinelEstimateTokenCount(bodyData.length);
        }
    }

    double cost = 0;
    if (provider && context.model) {
        NSNumber *inPrice = provider.pricePerThousandPromptTokens[context.model];
        NSNumber *outPrice = provider.pricePerThousandCompletionTokens[context.model];
        if (inPrice) {
            cost += (promptTokens / 1000.0) * inPrice.doubleValue;
        }
        if (outPrice) {
            cost += (completionTokens / 1000.0) * outPrice.doubleValue;
        }
    }

    NSString *budgetKey = context.budgetKey ?: host;
    NSTimeInterval now = [NSDate date].timeIntervalSince1970;

    os_unfair_lock_lock(&_lock);
    [self _locked_appendLedgerEntryForKey:budgetKey timestamp:now tokens:(promptTokens + completionTokens) cost:cost];
    LLMSentinelBudget *budget = self.budgets[budgetKey];
    BOOL overBudget = budget ? [self _locked_isOverBudgetForKey:budgetKey budget:budget reason:NULL] : NO;
    os_unfair_lock_unlock(&_lock);

    LLMSentinelUsageRecord *record = [[LLMSentinelUsageRecord alloc] initWithRequestID:[NSUUID UUID].UUIDString
                                                                                    host:host
                                                                      providerIdentifier:provider.identifier ?: @""
                                                                                   model:context.model
                                                                            promptTokens:promptTokens
                                                                        completionTokens:completionTokens
                                                                         tokensEstimated:estimated
                                                                                 costUSD:cost
                                                                                 latency:latency
                                                                          httpStatusCode:context.httpStatusCode
                                                                           requestFailed:failed
                                                              budgetExceededAfterRecord:overBudget
                                                                               budgetKey:budgetKey
                                                                         redactedHeaders:context.redactedHeaders];

    [self _appendRecordToLedgerFile:record];

    id<LLMNetworkCostSentinelDelegate> delegate = self.delegate;
    if ([delegate respondsToSelector:@selector(sentinel:didRecordUsage:)]) {
        dispatch_async(dispatch_get_main_queue(), ^{
            [delegate sentinel:self didRecordUsage:record];
        });
    }
}

#pragma mark Resume interception

/// Returns YES if this task was blocked here and must not be resumed for
/// real. Called from the swizzled `-resume` on every intercepted task class.
- (BOOL)_interceptResumeForTask:(NSURLSessionTask *)task {
    if (!self.active) {
        return NO;
    }
    NSURLRequest *request = task.currentRequest ?: task.originalRequest;
    if (!request) {
        return NO;
    }

    _LLMSentinelTaskContext *context = [self _contextForTask:task creatingFromRequest:request];
    if (!context) {
        return NO;
    }

    if (!context.startDate && [task respondsToSelector:@selector(delegate)]) {
        // Per-task delegates were added in iOS 15 / macOS 12, most notably
        // as the mechanism behind `URLSession.data(for:)` in Swift. Picking
        // it up here lets usage and cost tracking work on that path even
        // though, as explained below, hard blocking cannot.
        id perTaskDelegate = [task valueForKey:@"delegate"];
        if (perTaskDelegate) {
            [self _swizzleDelegateIfNeeded:perTaskDelegate];
        }
    }
    context.startDate = [NSDate date];

    NSString *reason = nil;
    BOOL overBudget = NO;
    LLMSentinelBudgetPolicy policy = LLMSentinelBudgetPolicyWarn;
    os_unfair_lock_lock(&_lock);
    LLMSentinelBudget *budget = self.budgets[context.budgetKey];
    if (budget) {
        overBudget = [self _locked_isOverBudgetForKey:context.budgetKey budget:budget reason:&reason];
        policy = budget.policy;
    }
    os_unfair_lock_unlock(&_lock);

    if (!overBudget || policy != LLMSentinelBudgetPolicyBlock) {
        return NO;
    }

    // Hard blocking is only possible on the completion handler creation
    // path, where this file owns the handler and can invoke it directly
    // with a synthetic error instead of ever letting the transfer start.
    // A delegate driven or async task has no equivalent this file can call
    // safely: synthesizing `URLSession:task:didCompleteWithError:` would
    // need the real `NSURLSession` instance, which a bare `NSURLSessionTask`
    // has no public way to hand back. Rather than pass a fabricated session
    // reference into code that did not expect one, such a task is allowed
    // through; it is still counted against the budget and still reported,
    // just not prevented. See the README for how to route around this.
    if (!context.completionHandler) {
        return NO;
    }

    void (^handler)(NSData *_Nullable, NSURLResponse *_Nullable, NSError *_Nullable) = context.completionHandler;
    os_unfair_lock_lock(&_lock);
    [self.taskContexts removeObjectForKey:task];
    os_unfair_lock_unlock(&_lock);

    NSError *blockedError = [NSError errorWithDomain:LLMSentinelErrorDomain
                                                 code:LLMSentinelErrorBudgetExceeded
                                             userInfo:@{NSLocalizedDescriptionKey: reason ?: @"LLM budget exceeded"}];

    id<LLMNetworkCostSentinelDelegate> delegate = self.delegate;
    NSString *budgetKey = context.budgetKey;
    if ([delegate respondsToSelector:@selector(sentinel:didBlockRequest:forBudgetKey:reason:)]) {
        dispatch_async(dispatch_get_main_queue(), ^{
            [delegate sentinel:self didBlockRequest:request forBudgetKey:budgetKey reason:reason ?: @""];
        });
    }

    if (handler) {
        handler(nil, nil, blockedError);
    }
    return YES;
}

#pragma mark Delegate wrapping

- (void)_swizzleDelegateIfNeeded:(id)delegate {
    if (!delegate) {
        return;
    }
    if (objc_getAssociatedObject(delegate, kLLMSentinelWrappedFlag)) {
        return;
    }
    Class currentClass = object_getClass(delegate);
    if ([NSStringFromClass(currentClass) hasPrefix:@"LLMSentinel_"]) {
        objc_setAssociatedObject(delegate, kLLMSentinelWrappedFlag, @YES, OBJC_ASSOCIATION_RETAIN);
        return;
    }
    Class subclass = _LLMSentinelDynamicSubclassFor(currentClass);
    if (subclass == currentClass) {
        return;
    }
    object_setClass(delegate, subclass);
    objc_setAssociatedObject(delegate, kLLMSentinelWrappedFlag, @YES, OBJC_ASSOCIATION_RETAIN);
}

#pragma mark Registration entry points used by the class/instance swizzles

- (void)_registerCreatedTask:(NSURLSessionTask *)task
                      request:(NSURLRequest *)request
            completionHandler:(nullable void (^)(NSData *_Nullable, NSURLResponse *_Nullable, NSError *_Nullable))completionHandler {
    if (!self.active) {
        return;
    }
    os_unfair_lock_lock(&_lock);
    _LLMSentinelTaskContext *context = [self _makeContextForRequest:request];
    if (context) {
        context.completionHandler = completionHandler;
        [self.taskContexts setObject:context forKey:task];
    }
    os_unfair_lock_unlock(&_lock);
}

#pragma mark Ledger file persistence

- (void)_openLedgerFileAndReplay:(NSURL *)url {
    if (url.isFileURL) {
        NSData *existing = [NSData dataWithContentsOfURL:url];
        if (existing.length > 0) {
            [self _replayLedgerData:existing];
        }
    }
    const char *path = url.fileSystemRepresentation;
    int fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0600);
    os_unfair_lock_lock(&_lock);
    _ledgerFileDescriptor = fd;
    os_unfair_lock_unlock(&_lock);
}

- (void)_replayLedgerData:(NSData *)data {
    NSString *text = [[NSString alloc] initWithData:data encoding:NSUTF8StringEncoding];
    if (text.length == 0) {
        return;
    }
    NSISO8601DateFormatter *formatter = [[NSISO8601DateFormatter alloc] init];
    os_unfair_lock_lock(&_lock);
    for (NSString *line in [text componentsSeparatedByString:@"\n"]) {
        if (line.length == 0) {
            continue;
        }
        NSData *lineData = [line dataUsingEncoding:NSUTF8StringEncoding];
        NSDictionary *entry = [NSJSONSerialization JSONObjectWithData:lineData options:0 error:NULL];
        if (![entry isKindOfClass:[NSDictionary class]]) {
            continue;
        }
        NSString *budgetKey = entry[@"budget_key"];
        NSString *timestampString = entry[@"timestamp"];
        NSNumber *promptTokens = entry[@"prompt_tokens"];
        NSNumber *completionTokens = entry[@"completion_tokens"];
        NSNumber *cost = entry[@"cost_usd"];
        if (!budgetKey || !timestampString) {
            continue;
        }
        NSDate *timestamp = [formatter dateFromString:timestampString];
        if (!timestamp) {
            continue;
        }
        NSUInteger tokens = promptTokens.unsignedIntegerValue + completionTokens.unsignedIntegerValue;
        [self _locked_appendLedgerEntryForKey:budgetKey
                                     timestamp:timestamp.timeIntervalSince1970
                                        tokens:tokens
                                          cost:cost.doubleValue];
    }
    os_unfair_lock_unlock(&_lock);
}

- (void)_appendRecordToLedgerFile:(LLMSentinelUsageRecord *)record {
    int fd;
    os_unfair_lock_lock(&_lock);
    fd = _ledgerFileDescriptor;
    os_unfair_lock_unlock(&_lock);
    if (fd < 0) {
        return;
    }

    NSISO8601DateFormatter *formatter = [[NSISO8601DateFormatter alloc] init];
    NSDictionary *line = @{
        @"request_id" : record.requestID,
        @"host" : record.host,
        @"provider" : record.providerIdentifier,
        @"model" : record.model ?: [NSNull null],
        @"prompt_tokens" : @(record.promptTokens),
        @"completion_tokens" : @(record.completionTokens),
        @"tokens_estimated" : @(record.tokensEstimated),
        @"cost_usd" : @(record.costUSD),
        @"latency_s" : @(record.latency),
        @"http_status" : @(record.httpStatusCode),
        @"failed" : @(record.requestFailed),
        @"budget_key" : record.budgetKey,
        @"timestamp" : [formatter stringFromDate:record.timestamp],
    };
    NSData *jsonData = [NSJSONSerialization dataWithJSONObject:line options:0 error:NULL];
    if (!jsonData) {
        return;
    }

    dispatch_async(self.ioQueue, ^{
        NSMutableData *toWrite = [jsonData mutableCopy];
        [toWrite appendBytes:"\n" length:1];
        write(fd, toWrite.bytes, toWrite.length);
        fsync(fd);
    });
}

#pragma mark Bundled providers

+ (LLMSentinelProvider *)openAIProvider {
    return [[LLMSentinelProvider alloc] initWithIdentifier:@"openai"
                                                 hostPattern:@"api\\.openai\\.com"
                                          promptTokenKeyPath:@[@"usage", @"prompt_tokens"]
                                      completionTokenKeyPath:@[@"usage", @"completion_tokens"]
                                pricePerThousandPromptTokens:@{@"gpt-4o" : @0.0025, @"gpt-4o-mini" : @0.00015}
                            pricePerThousandCompletionTokens:@{@"gpt-4o" : @0.01, @"gpt-4o-mini" : @0.0006}];
}

+ (LLMSentinelProvider *)anthropicProvider {
    return [[LLMSentinelProvider alloc] initWithIdentifier:@"anthropic"
                                                 hostPattern:@"api\\.anthropic\\.com"
                                          promptTokenKeyPath:@[@"usage", @"input_tokens"]
                                      completionTokenKeyPath:@[@"usage", @"output_tokens"]
                                pricePerThousandPromptTokens:@{@"claude-sonnet-4-5" : @0.003}
                            pricePerThousandCompletionTokens:@{@"claude-sonnet-4-5" : @0.015}];
}

@end

#pragma mark - C swizzle installers

static void _LLMSentinelSwizzleInstanceSelector(Class cls, SEL selector, IMP newIMP, IMP *outOriginal) {
    Method method = class_getInstanceMethod(cls, selector);
    if (!method) {
        return;
    }
    *outOriginal = method_getImplementation(method);
    class_replaceMethod(cls, selector, newIMP, method_getTypeEncoding(method));
}

static void _LLMSentinelSwizzleClassSelector(Class cls, SEL selector, IMP newIMP, IMP *outOriginal) {
    Class metaclass = object_getClass(cls);
    Method method = class_getClassMethod(cls, selector);
    if (!method) {
        return;
    }
    *outOriginal = method_getImplementation(method);
    class_replaceMethod(metaclass, selector, newIMP, method_getTypeEncoding(method));
}

static void _LLMSentinelInstallResumeSwizzle(Class cls) {
    if (!cls || objc_getAssociatedObject(cls, kLLMSentinelOrigResume)) {
        return;
    }
    Method method = class_getInstanceMethod(cls, @selector(resume));
    if (!method) {
        return;
    }
    IMP original = method_getImplementation(method);
    objc_setAssociatedObject(cls, kLLMSentinelOrigResume, [NSValue valueWithPointer:original], OBJC_ASSOCIATION_RETAIN);
    class_replaceMethod(cls, @selector(resume), (IMP)_LLMSentinel_resume, method_getTypeEncoding(method));
}

#pragma mark - Swizzled implementations

static NSURLSessionDataTask *_LLMSentinel_dataTaskWithRequestCompletionHandler(
    id self, SEL _cmd, NSURLRequest *request,
    void (^completionHandler)(NSData *_Nullable, NSURLResponse *_Nullable, NSError *_Nullable)) {
    __block NSURLSessionDataTask *taskRef = nil;
    void (^wrapped)(NSData *_Nullable, NSURLResponse *_Nullable, NSError *_Nullable) =
        ^(NSData *_Nullable data, NSURLResponse *_Nullable response, NSError *_Nullable error) {
            [[LLMNetworkCostSentinel sharedSentinel] _handleCompletionForTask:taskRef data:data response:response error:error];
            if (completionHandler) {
                completionHandler(data, response, error);
            }
        };

    NSURLSessionDataTask *task = ((NSURLSessionDataTask * (*)(id, SEL, NSURLRequest *, id))
                                       _LLMSentinelOriginalDataTaskWithRequestCompletionHandler)(self, _cmd, request, wrapped);
    taskRef = task;
    if (task) {
        [[LLMNetworkCostSentinel sharedSentinel] _registerCreatedTask:task request:request completionHandler:wrapped];
    }
    return task;
}

static NSURLSessionUploadTask *_LLMSentinel_uploadTaskWithRequestFromDataCompletionHandler(
    id self, SEL _cmd, NSURLRequest *request, NSData *_Nullable bodyData,
    void (^completionHandler)(NSData *_Nullable, NSURLResponse *_Nullable, NSError *_Nullable)) {
    __block NSURLSessionUploadTask *taskRef = nil;
    void (^wrapped)(NSData *_Nullable, NSURLResponse *_Nullable, NSError *_Nullable) =
        ^(NSData *_Nullable data, NSURLResponse *_Nullable response, NSError *_Nullable error) {
            [[LLMNetworkCostSentinel sharedSentinel] _handleCompletionForTask:taskRef data:data response:response error:error];
            if (completionHandler) {
                completionHandler(data, response, error);
            }
        };

    NSURLSessionUploadTask *task =
        ((NSURLSessionUploadTask * (*)(id, SEL, NSURLRequest *, NSData *, id))
             _LLMSentinelOriginalUploadTaskWithRequestFromDataCompletionHandler)(self, _cmd, request, bodyData, wrapped);
    taskRef = task;
    if (task) {
        // The upload body has already left as `bodyData`, distinct from the
        // request's own HTTPBody, so build the request based context by hand
        // instead of relying on `_makeContextForRequest:` inspecting
        // `request.HTTPBody`, which is nil for this call.
        NSMutableURLRequest *effectiveRequest = [request mutableCopy];
        if (!effectiveRequest.HTTPBody) {
            effectiveRequest.HTTPBody = bodyData;
        }
        [[LLMNetworkCostSentinel sharedSentinel] _registerCreatedTask:task request:effectiveRequest completionHandler:wrapped];
    }
    return task;
}

static NSURLSession *_LLMSentinel_sessionWithConfigurationDelegateDelegateQueue(id self, SEL _cmd,
                                                                                  NSURLSessionConfiguration *configuration,
                                                                                  id delegate, NSOperationQueue *queue) {
    NSURLSession *session = ((NSURLSession * (*)(id, SEL, id, id, id))
                                  _LLMSentinelOriginalSessionWithConfigurationDelegateDelegateQueue)(self, _cmd, configuration, delegate, queue);
    if (delegate) {
        [[LLMNetworkCostSentinel sharedSentinel] _swizzleDelegateIfNeeded:delegate];
    }
    return session;
}

static void _LLMSentinel_didReceiveResponse(id self, SEL _cmd, NSURLSession *session, NSURLSessionDataTask *task,
                                             NSURLResponse *response, void (^completionHandler)(NSURLSessionResponseDisposition)) {
    [[LLMNetworkCostSentinel sharedSentinel] _observeResponse:response forTask:task];
    NSValue *originalValue = objc_getAssociatedObject(object_getClass(self), kLLMSentinelOrigDidReceiveResponse);
    IMP original = originalValue.pointerValue;
    if (original) {
        ((void (*)(id, SEL, NSURLSession *, NSURLSessionDataTask *, NSURLResponse *, id))original)(self, _cmd, session, task, response,
                                                                                                     completionHandler);
    } else if (completionHandler) {
        completionHandler(NSURLSessionResponseAllow);
    }
}

static void _LLMSentinel_didReceiveData(id self, SEL _cmd, NSURLSession *session, NSURLSessionDataTask *task, NSData *data) {
    [[LLMNetworkCostSentinel sharedSentinel] _observeReceivedData:data forTask:task];
    NSValue *originalValue = objc_getAssociatedObject(object_getClass(self), kLLMSentinelOrigDidReceiveData);
    IMP original = originalValue.pointerValue;
    if (original) {
        ((void (*)(id, SEL, NSURLSession *, NSURLSessionDataTask *, NSData *))original)(self, _cmd, session, task, data);
    }
}

static void _LLMSentinel_didComplete(id self, SEL _cmd, NSURLSession *session, NSURLSessionTask *task, NSError *_Nullable error) {
    [[LLMNetworkCostSentinel sharedSentinel] _observeTaskDidComplete:task error:error];
    NSValue *originalValue = objc_getAssociatedObject(object_getClass(self), kLLMSentinelOrigDidComplete);
    IMP original = originalValue.pointerValue;
    if (original) {
        ((void (*)(id, SEL, NSURLSession *, NSURLSessionTask *, NSError *))original)(self, _cmd, session, task, error);
    }
}

static void _LLMSentinel_resume(id self, SEL _cmd) {
    BOOL blocked = [[LLMNetworkCostSentinel sharedSentinel] _interceptResumeForTask:(NSURLSessionTask *)self];
    if (blocked) {
        return;
    }
    Class cls = object_getClass(self);
    IMP original = NULL;
    while (cls && !original) {
        NSValue *value = objc_getAssociatedObject(cls, kLLMSentinelOrigResume);
        if (value) {
            original = value.pointerValue;
            break;
        }
        cls = class_getSuperclass(cls);
    }
    if (original) {
        ((void (*)(id, SEL))original)(self, _cmd);
    }
}
