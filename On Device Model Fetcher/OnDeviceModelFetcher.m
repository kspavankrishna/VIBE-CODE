#import "OnDeviceModelFetcher.h"

#import <CommonCrypto/CommonDigest.h>
#import <fcntl.h>
#import <unistd.h>

NSErrorDomain const ODMFetcherErrorDomain = @"ODMFetcher";

static const NSUInteger kODMHashChunkBytes = 1u << 20;
static const NSTimeInterval kODMProgressInterval = 0.25;
static const uint64_t kODMDefaultHeadroomFloor = 256ull * 1024 * 1024;
static NSString *const kODMPartSuffix = @".part";
static NSString *const kODMMetaSuffix = @".part.meta";

static NSError *ODMMakeError(ODMFetcherErrorCode code, NSString *message, NSError *underlying) {
    NSMutableDictionary *info = [NSMutableDictionary dictionaryWithObject:message forKey:NSLocalizedDescriptionKey];
    if (underlying) info[NSUnderlyingErrorKey] = underlying;
    return [NSError errorWithDomain:ODMFetcherErrorDomain code:code userInfo:info];
}

static NSString *ODMHex(const unsigned char *bytes, size_t length) {
    static const char digits[] = "0123456789abcdef";
    char buffer[CC_SHA256_DIGEST_LENGTH * 2 + 1];
    for (size_t i = 0; i < length; i++) {
        buffer[i * 2] = digits[bytes[i] >> 4];
        buffer[i * 2 + 1] = digits[bytes[i] & 0x0f];
    }
    buffer[length * 2] = '\0';
    return [NSString stringWithUTF8String:buffer];
}

#pragma mark - ODMFetchSpec

@implementation ODMFetchSpec

- (instancetype)initWithSourceURL:(NSURL *)sourceURL
                        sha256Hex:(NSString *)sha256Hex
                    expectedBytes:(uint64_t)expectedBytes
                         fileName:(NSString *)fileName
                            error:(NSError **)error {
    self = [super init];
    if (!self) return nil;

    NSString *scheme = sourceURL.scheme.lowercaseString;
    NSString *digest = sha256Hex.lowercaseString;
    NSCharacterSet *nonHex = [[NSCharacterSet characterSetWithCharactersInString:@"0123456789abcdef"] invertedSet];
    NSString *problem = nil;
    if (![scheme isEqualToString:@"https"]) {
        problem = @"sourceURL must be https";
    } else if (digest.length != 64 || [digest rangeOfCharacterFromSet:nonHex].location != NSNotFound) {
        problem = @"sha256Hex must be 64 hex characters";
    } else if (expectedBytes == 0) {
        problem = @"expectedBytes must be greater than zero";
    } else if (fileName.length == 0 || fileName.length > 200 || [fileName hasPrefix:@"."] ||
               [fileName rangeOfString:@"/"].location != NSNotFound ||
               [fileName rangeOfString:@"\\"].location != NSNotFound ||
               [fileName rangeOfString:@"\0"].location != NSNotFound) {
        problem = @"fileName must be a plain leaf name";
    }
    if (problem) {
        if (error) *error = ODMMakeError(ODMFetcherErrorInvalidSpec, problem, nil);
        return nil;
    }
    _sourceURL = [sourceURL copy];
    _sha256Hex = [digest copy];
    _expectedBytes = expectedBytes;
    _fileName = [fileName copy];
    return self;
}

- (id)copyWithZone:(NSZone *)zone {
    return self;
}

@end

#pragma mark - ODMModelFetcher

@interface ODMModelFetcher () <NSURLSessionDataDelegate> {
    dispatch_queue_t _queue;
    NSOperationQueue *_delegateQueue;
}
@property (nonatomic, copy, readwrite) NSURL *directoryURL;
@end

@implementation ODMModelFetcher {
    // Everything below is touched only on _queue.
    ODMFetchSpec *_spec;
    ODMProgressHandler _progressHandler;
    ODMCompletionHandler _completionHandler;
    NSURLSession *_session;
    NSURLSessionDataTask *_task;
    NSFileHandle *_handle;
    CC_SHA256_CTX _hashContext;
    uint64_t _bytesOnDisk;
    NSString *_etag;
    NSString *_lastModified;
    NSUInteger _attempt;
    NSUInteger _generation;
    BOOL _running;
    BOOL _responseAccepted;
    BOOL _restartedForRangeMismatch;
    NSTimeInterval _retryAfterHint;
    NSInteger _lastStatus;
    NSTimeInterval _lastProgressAt;
}

- (instancetype)initWithDirectoryURL:(NSURL *)directoryURL error:(NSError **)error {
    self = [super init];
    if (!self) return nil;
    NSError *fsError = nil;
    if (![[NSFileManager defaultManager] createDirectoryAtURL:directoryURL
                                  withIntermediateDirectories:YES
                                                   attributes:nil
                                                        error:&fsError]) {
        if (error) *error = ODMMakeError(ODMFetcherErrorFilesystem, @"cannot create model directory", fsError);
        return nil;
    }
    _directoryURL = [directoryURL copy];
    _maxAttempts = 6;
    _baseBackoffSeconds = 1.5;
    _maxBackoffSeconds = 60;
    _diskHeadroomFraction = 0.05;
    _diskHeadroomFloorBytes = kODMDefaultHeadroomFloor;
    _allowsCellularAccess = NO;
    _queue = dispatch_queue_create("odm.fetcher", DISPATCH_QUEUE_SERIAL);
    _delegateQueue = [[NSOperationQueue alloc] init];
    _delegateQueue.maxConcurrentOperationCount = 1;
    _delegateQueue.underlyingQueue = _queue;
    return self;
}

#pragma mark Pure helpers

+ (NSTimeInterval)backoffForAttempt:(NSUInteger)attempt
                               base:(NSTimeInterval)base
                                cap:(NSTimeInterval)cap
                         randomUnit:(double)randomUnit {
    // Full jitter: uniform in [0, min(cap, base * 2^(attempt-1))]. A floor of
    // a quarter of the ceiling stops a lucky zero from hammering a struggling CDN.
    NSUInteger exponent = attempt > 0 ? MIN(attempt - 1, (NSUInteger)30) : 0;
    double ceiling = MIN(cap, base * pow(2.0, (double)exponent));
    double unit = MIN(1.0, MAX(0.0, randomUnit));
    return ceiling * (0.25 + 0.75 * unit);
}

+ (NSTimeInterval)retryAfterSecondsFromHeader:(NSString *)value now:(NSDate *)now {
    NSString *trimmed = [value stringByTrimmingCharactersInSet:[NSCharacterSet whitespaceCharacterSet]];
    if (trimmed.length == 0) return 0;
    NSScanner *scanner = [NSScanner scannerWithString:trimmed];
    long long seconds = 0;
    if ([scanner scanLongLong:&seconds] && scanner.isAtEnd) {
        return seconds > 0 ? (NSTimeInterval)seconds : 0;
    }
    NSDateFormatter *formatter = [[NSDateFormatter alloc] init];
    formatter.locale = [NSLocale localeWithLocaleIdentifier:@"en_US_POSIX"];
    formatter.timeZone = [NSTimeZone timeZoneForSecondsFromGMT:0];
    formatter.dateFormat = @"EEE, dd MMM yyyy HH:mm:ss zzz";
    NSDate *date = [formatter dateFromString:trimmed];
    if (!date) return 0;
    return MAX(0, [date timeIntervalSinceDate:now]);
}

/// Parses "bytes 100-199/200". Returns NO for anything else, including "*" totals.
static BOOL ODMParseContentRange(NSString *header, uint64_t *start, uint64_t *end, uint64_t *total) {
    if (![header hasPrefix:@"bytes "]) return NO;
    NSString *body = [header substringFromIndex:6];
    NSArray<NSString *> *halves = [body componentsSeparatedByString:@"/"];
    if (halves.count != 2) return NO;
    NSArray<NSString *> *span = [halves[0] componentsSeparatedByString:@"-"];
    if (span.count != 2) return NO;
    NSScanner *a = [NSScanner scannerWithString:span[0]];
    NSScanner *b = [NSScanner scannerWithString:span[1]];
    NSScanner *c = [NSScanner scannerWithString:halves[1]];
    unsigned long long s = 0, e = 0, t = 0;
    if (!([a scanUnsignedLongLong:&s] && a.isAtEnd && [b scanUnsignedLongLong:&e] && b.isAtEnd &&
          [c scanUnsignedLongLong:&t] && c.isAtEnd)) {
        return NO;
    }
    if (e < s || e >= t) return NO;
    *start = s; *end = e; *total = t;
    return YES;
}

#pragma mark File layout

- (NSURL *)finalURLForSpec:(ODMFetchSpec *)spec {
    return [self.directoryURL URLByAppendingPathComponent:spec.fileName isDirectory:NO];
}

- (NSURL *)partURLForSpec:(ODMFetchSpec *)spec {
    return [self.directoryURL URLByAppendingPathComponent:[spec.fileName stringByAppendingString:kODMPartSuffix]];
}

- (NSURL *)metaURLForSpec:(ODMFetchSpec *)spec {
    return [self.directoryURL URLByAppendingPathComponent:[spec.fileName stringByAppendingString:kODMMetaSuffix]];
}

- (NSURL *)verifiedURLForSpec:(ODMFetchSpec *)spec {
    // A finished file only ever appears by atomic rename after the digest matched,
    // so existence plus exact size is enough to trust it on the fast path.
    NSURL *url = [self finalURLForSpec:spec];
    NSDictionary *attributes = [[NSFileManager defaultManager] attributesOfItemAtPath:url.path error:nil];
    if (attributes && [attributes fileSize] == spec.expectedBytes) return url;
    return nil;
}

- (void)discardPartialForSpec:(ODMFetchSpec *)spec {
    NSFileManager *fm = [NSFileManager defaultManager];
    [fm removeItemAtURL:[self partURLForSpec:spec] error:nil];
    [fm removeItemAtURL:[self metaURLForSpec:spec] error:nil];
}

- (void)writeMeta {
    NSMutableDictionary *meta = [@{
        @"url": _spec.sourceURL.absoluteString,
        @"sha256": _spec.sha256Hex,
        @"bytes": @(_spec.expectedBytes),
    } mutableCopy];
    if (_etag) meta[@"etag"] = _etag;
    if (_lastModified) meta[@"lastModified"] = _lastModified;
    NSData *data = [NSJSONSerialization dataWithJSONObject:meta options:0 error:nil];
    [data writeToURL:[self metaURLForSpec:_spec] options:NSDataWritingAtomic error:nil];
}

/// Loads validators from the sidecar. Returns NO when the sidecar describes a
/// different artifact, in which case the caller must throw the partial away.
- (BOOL)loadMetaMatchingSpec {
    NSData *data = [NSData dataWithContentsOfURL:[self metaURLForSpec:_spec]];
    if (!data) return NO;
    NSDictionary *meta = [NSJSONSerialization JSONObjectWithData:data options:0 error:nil];
    if (![meta isKindOfClass:[NSDictionary class]]) return NO;
    if (![meta[@"url"] isEqual:_spec.sourceURL.absoluteString]) return NO;
    if (![meta[@"sha256"] isEqual:_spec.sha256Hex]) return NO;
    if ([meta[@"bytes"] unsignedLongLongValue] != _spec.expectedBytes) return NO;
    _etag = meta[@"etag"];
    _lastModified = meta[@"lastModified"];
    return YES;
}

- (uint64_t)availableBytesAtDirectory {
    NSArray *keys = @[NSURLVolumeAvailableCapacityForImportantUsageKey, NSURLVolumeAvailableCapacityKey];
    NSDictionary *values = [self.directoryURL resourceValuesForKeys:keys error:nil];
    NSNumber *important = values[NSURLVolumeAvailableCapacityForImportantUsageKey];
    if (important && important.longLongValue > 0) return important.unsignedLongLongValue;
    return [values[NSURLVolumeAvailableCapacityKey] unsignedLongLongValue];
}

#pragma mark Public entry points

- (BOOL)fetchSpec:(ODMFetchSpec *)spec
         progress:(ODMProgressHandler)progress
       completion:(ODMCompletionHandler)completion
            error:(NSError **)error {
    __block BOOL accepted = NO;
    dispatch_sync(_queue, ^{
        if (self->_running) return;
        self->_running = YES;
        accepted = YES;
        self->_spec = spec;
        self->_progressHandler = [progress copy];
        self->_completionHandler = [completion copy];
        self->_attempt = 0;
        self->_generation += 1;
        self->_restartedForRangeMismatch = NO;
        self->_retryAfterHint = 0;
        self->_lastProgressAt = 0;
        NSUInteger generation = self->_generation;
        dispatch_async(self->_queue, ^{ [self beginRunForGeneration:generation]; });
    });
    if (!accepted && error) {
        *error = ODMMakeError(ODMFetcherErrorBusy, @"a fetch is already running on this instance", nil);
    }
    return accepted;
}

- (void)cancel {
    dispatch_async(_queue, ^{
        if (!self->_running) return;
        self->_generation += 1;
        [self->_task cancel];
        [self closeHandle];
        [self finishWithURL:nil error:ODMMakeError(ODMFetcherErrorCancelled, @"cancelled, partial file kept for resume", nil)];
    });
}

#pragma mark Run state machine (all on _queue)

- (void)beginRunForGeneration:(NSUInteger)generation {
    if (generation != _generation) return;
    NSURL *existing = [self verifiedURLForSpec:_spec];
    if (existing) {
        [self finishWithURL:existing error:nil];
        return;
    }

    NSFileManager *fm = [NSFileManager defaultManager];
    NSURL *partURL = [self partURLForSpec:_spec];
    BOOL hasPart = [fm fileExistsAtPath:partURL.path];
    if (hasPart && ![self loadMetaMatchingSpec]) {
        // Different artifact, missing validators or a torn sidecar: never splice bytes across them.
        [self discardPartialForSpec:_spec];
        hasPart = NO;
    }
    if (!hasPart) {
        _etag = nil;
        _lastModified = nil;
        [fm createFileAtPath:partURL.path contents:nil attributes:nil];
    }

    uint64_t onDisk = [[fm attributesOfItemAtPath:partURL.path error:nil] fileSize];
    if (onDisk > _spec.expectedBytes) {
        [self discardPartialForSpec:_spec];
        [fm createFileAtPath:partURL.path contents:nil attributes:nil];
        onDisk = 0;
    }

    uint64_t remaining = _spec.expectedBytes - onDisk;
    uint64_t headroom = MAX(_diskHeadroomFloorBytes, (uint64_t)((double)_spec.expectedBytes * _diskHeadroomFraction));
    uint64_t available = [self availableBytesAtDirectory];
    if (available < remaining + headroom) {
        NSString *message = [NSString stringWithFormat:@"need %llu bytes free (%llu remaining plus %llu headroom), have %llu",
                             remaining + headroom, remaining, headroom, available];
        [self finishWithURL:nil error:ODMMakeError(ODMFetcherErrorInsufficientDisk, message, nil)];
        return;
    }

    if (![self rehashPartAndOpenHandle]) return;

    NSURLSessionConfiguration *config = [NSURLSessionConfiguration ephemeralSessionConfiguration];
    config.allowsCellularAccess = _allowsCellularAccess;
    config.waitsForConnectivity = YES;
    config.timeoutIntervalForRequest = 45;
    config.timeoutIntervalForResource = 60 * 60 * 12;
    config.HTTPAdditionalHeaders = @{ @"Accept-Encoding": @"identity" };   // ranges and compression do not mix
    _session = [NSURLSession sessionWithConfiguration:config delegate:self delegateQueue:_delegateQueue];
    [self startAttempt];
}

/// Re-reads the existing partial file through SHA-256 so the running digest is
/// exact before any new byte is appended.
- (BOOL)rehashPartAndOpenHandle {
    NSURL *partURL = [self partURLForSpec:_spec];
    NSError *openError = nil;
    NSFileHandle *reader = [NSFileHandle fileHandleForReadingFromURL:partURL error:&openError];
    if (!reader) {
        [self finishWithURL:nil error:ODMMakeError(ODMFetcherErrorFilesystem, @"cannot open partial file", openError)];
        return NO;
    }
    CC_SHA256_Init(&_hashContext);
    uint64_t total = 0;
    uint64_t size = [[[NSFileManager defaultManager] attributesOfItemAtPath:partURL.path error:nil] fileSize];
    [self emitProgressPhase:ODMFetchPhaseHashingPrefix force:YES backoff:0];
    for (;;) {
        @autoreleasepool {
            NSData *chunk = [reader readDataUpToLength:kODMHashChunkBytes error:nil];
            if (chunk.length == 0) break;
            CC_SHA256_Update(&_hashContext, chunk.bytes, (CC_LONG)chunk.length);
            total += chunk.length;
        }
    }
    [reader closeFile];
    if (total != size) {
        [self finishWithURL:nil error:ODMMakeError(ODMFetcherErrorFilesystem, @"partial file changed while hashing", nil)];
        return NO;
    }
    NSFileHandle *writer = [NSFileHandle fileHandleForWritingToURL:partURL error:&openError];
    if (!writer) {
        [self finishWithURL:nil error:ODMMakeError(ODMFetcherErrorFilesystem, @"cannot open partial file for append", openError)];
        return NO;
    }
    [writer seekToEndReturningOffset:NULL error:nil];
    _handle = writer;
    _bytesOnDisk = total;
    return YES;
}

- (void)closeHandle {
    if (_handle) {
        [_handle closeAndReturnError:nil];
        _handle = nil;
    }
}

- (void)resetPartialToEmpty {
    [_handle truncateAtOffset:0 error:nil];
    [_handle seekToOffset:0 error:nil];
    CC_SHA256_Init(&_hashContext);
    _bytesOnDisk = 0;
}

- (void)startAttempt {
    _attempt += 1;
    _responseAccepted = NO;
    _lastStatus = 0;
    _retryAfterHint = 0;

    NSMutableURLRequest *request = [NSMutableURLRequest requestWithURL:_spec.sourceURL];
    request.HTTPMethod = @"GET";
    request.cachePolicy = NSURLRequestReloadIgnoringLocalCacheData;
    [_spec.extraHeaders enumerateKeysAndObjectsUsingBlock:^(NSString *key, NSString *value, BOOL *stop) {
        [request setValue:value forHTTPHeaderField:key];
    }];
    if (_bytesOnDisk > 0) {
        [request setValue:[NSString stringWithFormat:@"bytes=%llu-", _bytesOnDisk] forHTTPHeaderField:@"Range"];
        // If-Range makes the server answer 200 with the whole body when the object
        // changed, instead of splicing two different files together.
        NSString *validator = _etag ?: _lastModified;
        if (validator) [request setValue:validator forHTTPHeaderField:@"If-Range"];
    }
    _task = [_session dataTaskWithRequest:request];
    [self emitProgressPhase:ODMFetchPhaseDownloading force:YES backoff:0];
    [_task resume];
}

#pragma mark NSURLSessionDataDelegate

- (void)URLSession:(NSURLSession *)session
          dataTask:(NSURLSessionDataTask *)dataTask
didReceiveResponse:(NSURLResponse *)response
 completionHandler:(void (^)(NSURLSessionResponseDisposition))completionHandler {
    if (dataTask != _task) { completionHandler(NSURLSessionResponseCancel); return; }

    NSHTTPURLResponse *http = [response isKindOfClass:[NSHTTPURLResponse class]] ? (NSHTTPURLResponse *)response : nil;
    _lastStatus = http.statusCode;
    NSString *retryAfter = http.allHeaderFields[@"Retry-After"];
    _retryAfterHint = [ODMModelFetcher retryAfterSecondsFromHeader:retryAfter now:[NSDate date]];

    if (http.statusCode == 200) {
        if (_bytesOnDisk > 0) [self resetPartialToEmpty];     // range ignored or object replaced
        if (http.expectedContentLength > 0 && (uint64_t)http.expectedContentLength != _spec.expectedBytes) {
            _responseAccepted = NO;
            completionHandler(NSURLSessionResponseCancel);
            [self failPermanently:ODMFetcherErrorSizeMismatch
                          message:[NSString stringWithFormat:@"server reports %lld bytes, spec says %llu",
                                   http.expectedContentLength, _spec.expectedBytes]];
            return;
        }
        [self adoptValidatorsFromResponse:http];
        _responseAccepted = YES;
        completionHandler(NSURLSessionResponseAllow);
        return;
    }

    if (http.statusCode == 206) {
        uint64_t start = 0, end = 0, total = 0;
        NSString *contentRange = http.allHeaderFields[@"Content-Range"];
        BOOL parsed = contentRange && ODMParseContentRange(contentRange, &start, &end, &total);
        if (!parsed || start != _bytesOnDisk || total != _spec.expectedBytes) {
            completionHandler(NSURLSessionResponseCancel);
            [self handleBadRangeResponse];
            return;
        }
        [self adoptValidatorsFromResponse:http];
        _responseAccepted = YES;
        completionHandler(NSURLSessionResponseAllow);
        return;
    }

    if (http.statusCode == 416) {
        completionHandler(NSURLSessionResponseCancel);
        if (_bytesOnDisk == _spec.expectedBytes) {
            [self verifyAndPublish];
        } else {
            [self handleBadRangeResponse];
        }
        return;
    }

    completionHandler(NSURLSessionResponseCancel);
    NSInteger status = http.statusCode;
    BOOL transient = status == 408 || status == 425 || status == 429 || status >= 500;
    if (transient) {
        [self scheduleRetryWithReason:[NSString stringWithFormat:@"HTTP %ld", (long)status]];
    } else {
        [self failPermanently:ODMFetcherErrorHTTPStatus
                      message:[NSString stringWithFormat:@"HTTP %ld from %@", (long)status, _spec.sourceURL.host]];
    }
}

- (void)adoptValidatorsFromResponse:(NSHTTPURLResponse *)http {
    NSString *etag = http.allHeaderFields[@"ETag"];
    // Weak validators are not allowed in If-Range, so only keep strong ones.
    NSString *strong = (etag.length > 0 && ![etag hasPrefix:@"W/"]) ? etag : nil;
    NSString *modified = http.allHeaderFields[@"Last-Modified"];
    if (_bytesOnDisk == 0 || strong || modified) {
        _etag = strong ?: _etag;
        _lastModified = modified ?: _lastModified;
        [self writeMeta];
    }
}

- (void)handleBadRangeResponse {
    if (_restartedForRangeMismatch) {
        [self failPermanently:ODMFetcherErrorBadRangeResponse message:@"server keeps returning ranges that do not line up"];
        return;
    }
    _restartedForRangeMismatch = YES;
    [self resetPartialToEmpty];
    _etag = nil;
    _lastModified = nil;
    [self writeMeta];
    [self scheduleRetryWithReason:@"range mismatch, restarting from zero"];
}

- (void)URLSession:(NSURLSession *)session dataTask:(NSURLSessionDataTask *)dataTask didReceiveData:(NSData *)data {
    if (dataTask != _task || !_responseAccepted) return;
    if (_bytesOnDisk + data.length > _spec.expectedBytes) {
        _responseAccepted = NO;
        [dataTask cancel];
        [self discardPartialForSpec:_spec];
        [self failPermanently:ODMFetcherErrorSizeMismatch message:@"server sent more bytes than the spec allows"];
        return;
    }
    NSError *writeError = nil;
    if (![_handle writeData:data error:&writeError]) {
        _responseAccepted = NO;
        [dataTask cancel];
        [self failPermanently:ODMFetcherErrorFilesystem
                      message:[NSString stringWithFormat:@"write failed: %@", writeError.localizedDescription]];
        return;
    }
    CC_SHA256_Update(&_hashContext, data.bytes, (CC_LONG)data.length);
    _bytesOnDisk += data.length;
    [self emitProgressPhase:ODMFetchPhaseDownloading force:NO backoff:0];
}

- (void)URLSession:(NSURLSession *)session task:(NSURLSessionTask *)task didCompleteWithError:(NSError *)error {
    if (task != _task) return;
    _task = nil;
    if (!_running) return;

    if (!_responseAccepted) {
        // A status handler above already decided what to do and cancelled the task.
        return;
    }
    if (error) {
        if (error.code == NSURLErrorCancelled) return;
        if ([self isRetryableTransportError:error]) {
            [self scheduleRetryWithReason:error.localizedDescription];
        } else {
            [self failPermanently:ODMFetcherErrorHTTPStatus message:error.localizedDescription];
        }
        return;
    }
    if (_bytesOnDisk < _spec.expectedBytes) {
        // Clean close short of the promised length: a proxy or CDN cut the body.
        [self scheduleRetryWithReason:@"connection closed before the last byte"];
        return;
    }
    [self verifyAndPublish];
}

- (BOOL)isRetryableTransportError:(NSError *)error {
    if (![error.domain isEqualToString:NSURLErrorDomain]) return NO;
    switch (error.code) {
        case NSURLErrorTimedOut:
        case NSURLErrorNetworkConnectionLost:
        case NSURLErrorNotConnectedToInternet:
        case NSURLErrorCannotConnectToHost:
        case NSURLErrorCannotFindHost:
        case NSURLErrorDNSLookupFailed:
        case NSURLErrorResourceUnavailable:
        case NSURLErrorInternationalRoamingOff:
        case NSURLErrorDataNotAllowed:
        case NSURLErrorSecureConnectionFailed:
            return YES;
        default:
            return NO;
    }
}

#pragma mark Retry, verify, publish

- (void)scheduleRetryWithReason:(NSString *)reason {
    if (_attempt >= _maxAttempts) {
        [self failPermanently:ODMFetcherErrorRetriesExhausted
                      message:[NSString stringWithFormat:@"gave up after %lu attempts, last: %@", (unsigned long)_attempt, reason]];
        return;
    }
    double random = (double)arc4random() / (double)UINT32_MAX;
    NSTimeInterval delay = [ODMModelFetcher backoffForAttempt:_attempt base:_baseBackoffSeconds cap:_maxBackoffSeconds randomUnit:random];
    // A server that says when to come back wins over our own guess, within the cap.
    delay = MIN(_maxBackoffSeconds, MAX(delay, _retryAfterHint));
    [self emitProgressPhase:ODMFetchPhaseBackoff force:YES backoff:delay];

    NSUInteger generation = _generation;
    dispatch_after(dispatch_time(DISPATCH_TIME_NOW, (int64_t)(delay * NSEC_PER_SEC)), _queue, ^{
        if (generation != self->_generation || !self->_running) return;
        [self startAttempt];
    });
}

- (void)verifyAndPublish {
    [self emitProgressPhase:ODMFetchPhaseVerifying force:YES backoff:0];
    CC_SHA256_CTX copy = _hashContext;
    unsigned char digest[CC_SHA256_DIGEST_LENGTH];
    CC_SHA256_Final(digest, &copy);
    NSString *actual = ODMHex(digest, sizeof digest);

    if (_bytesOnDisk != _spec.expectedBytes) {
        [self failPermanently:ODMFetcherErrorSizeMismatch message:@"final size differs from spec"];
        return;
    }
    if (![actual isEqualToString:_spec.sha256Hex]) {
        [self closeHandle];
        [self discardPartialForSpec:_spec];     // corrupt bytes must never be resumed
        [self finishWithURL:nil error:ODMMakeError(ODMFetcherErrorDigestMismatch,
            [NSString stringWithFormat:@"sha256 mismatch: expected %@, got %@", _spec.sha256Hex, actual], nil)];
        return;
    }

    // Flush to the platform, not just the page cache, before the rename makes the file visible.
    int fd = (int)_handle.fileDescriptor;
    if (fcntl(fd, F_FULLFSYNC) == -1) fsync(fd);
    [self closeHandle];

    NSFileManager *fm = [NSFileManager defaultManager];
    NSURL *finalURL = [self finalURLForSpec:_spec];
    NSError *moveError = nil;
    [fm removeItemAtURL:finalURL error:nil];
    if (![fm moveItemAtURL:[self partURLForSpec:_spec] toURL:finalURL error:&moveError]) {
        [self finishWithURL:nil error:ODMMakeError(ODMFetcherErrorFilesystem, @"cannot move verified file into place", moveError)];
        return;
    }
    NSURL *mutableURL = finalURL;
    [mutableURL setResourceValue:@YES forKey:NSURLIsExcludedFromBackupKey error:nil];   // weights are re-downloadable
    [fm removeItemAtURL:[self metaURLForSpec:_spec] error:nil];
    [self finishWithURL:finalURL error:nil];
}

- (void)failPermanently:(ODMFetcherErrorCode)code message:(NSString *)message {
    [self closeHandle];
    [self finishWithURL:nil error:ODMMakeError(code, message, nil)];
}

- (void)finishWithURL:(NSURL *)url error:(NSError *)error {
    if (!_running) return;
    _running = NO;
    _generation += 1;
    [_session invalidateAndCancel];
    _session = nil;
    _task = nil;
    [self closeHandle];
    ODMCompletionHandler completion = _completionHandler;
    ODMProgressHandler progress = _progressHandler;
    uint64_t total = _spec.expectedBytes;
    uint64_t onDisk = url ? total : _bytesOnDisk;
    NSUInteger attempt = _attempt;
    _completionHandler = nil;
    _progressHandler = nil;
    dispatch_async(dispatch_get_main_queue(), ^{
        if (progress && url) {
            ODMFetchProgress p = { ODMFetchPhaseFinished, onDisk, total, attempt, 0 };
            progress(p);
        }
        if (completion) completion(url, error);
    });
}

- (void)emitProgressPhase:(ODMFetchPhase)phase force:(BOOL)force backoff:(NSTimeInterval)backoff {
    ODMProgressHandler handler = _progressHandler;
    if (!handler) return;
    NSTimeInterval now = [NSProcessInfo processInfo].systemUptime;
    if (!force && now - _lastProgressAt < kODMProgressInterval) return;
    _lastProgressAt = now;
    ODMFetchProgress p = { phase, _bytesOnDisk, _spec.expectedBytes, _attempt, backoff };
    dispatch_async(dispatch_get_main_queue(), ^{ handler(p); });
}

@end
