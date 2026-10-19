#import <Foundation/Foundation.h>

NS_ASSUME_NONNULL_BEGIN

extern NSErrorDomain const ODMFetcherErrorDomain;

typedef NS_ERROR_ENUM(ODMFetcherErrorDomain, ODMFetcherErrorCode) {
    ODMFetcherErrorInvalidSpec = 1,
    ODMFetcherErrorInsufficientDisk = 2,
    ODMFetcherErrorHTTPStatus = 3,
    ODMFetcherErrorSizeMismatch = 4,
    ODMFetcherErrorDigestMismatch = 5,
    ODMFetcherErrorCancelled = 6,
    ODMFetcherErrorRetriesExhausted = 7,
    ODMFetcherErrorBadRangeResponse = 8,
    ODMFetcherErrorFilesystem = 9,
    ODMFetcherErrorBusy = 10,
};

/// Describes one model artifact. The digest and size are mandatory: a
/// download that cannot be verified is not a download we are willing to load.
@interface ODMFetchSpec : NSObject <NSCopying>
@property (nonatomic, copy, readonly) NSURL *sourceURL;
@property (nonatomic, copy, readonly) NSString *sha256Hex;       // 64 lowercase hex chars
@property (nonatomic, readonly) uint64_t expectedBytes;
@property (nonatomic, copy, readonly) NSString *fileName;        // leaf name, no separators
@property (nonatomic, copy, nullable) NSDictionary<NSString *, NSString *> *extraHeaders;

- (nullable instancetype)initWithSourceURL:(NSURL *)sourceURL
                                 sha256Hex:(NSString *)sha256Hex
                             expectedBytes:(uint64_t)expectedBytes
                                  fileName:(NSString *)fileName
                                     error:(NSError **)error NS_DESIGNATED_INITIALIZER;
- (instancetype)init NS_UNAVAILABLE;
@end

typedef NS_ENUM(NSInteger, ODMFetchPhase) {
    ODMFetchPhasePreflight,
    ODMFetchPhaseHashingPrefix,
    ODMFetchPhaseDownloading,
    ODMFetchPhaseVerifying,
    ODMFetchPhaseBackoff,
    ODMFetchPhaseFinished,
};

typedef struct {
    ODMFetchPhase phase;
    uint64_t bytesOnDisk;
    uint64_t totalBytes;
    NSUInteger attempt;
    NSTimeInterval backoffSeconds;
} ODMFetchProgress;

typedef void (^ODMProgressHandler)(ODMFetchProgress progress);
typedef void (^ODMCompletionHandler)(NSURL *_Nullable finalURL, NSError *_Nullable error);

/// Resumable, hash verified, disk aware downloader for multi gigabyte model
/// weights on iOS and macOS. One instance owns one directory and fetches one
/// spec at a time.
@interface ODMModelFetcher : NSObject

@property (nonatomic, copy, readonly) NSURL *directoryURL;
@property (nonatomic) NSUInteger maxAttempts;                 // default 6
@property (nonatomic) NSTimeInterval baseBackoffSeconds;      // default 1.5
@property (nonatomic) NSTimeInterval maxBackoffSeconds;       // default 60
@property (nonatomic) double diskHeadroomFraction;            // default 0.05 of expectedBytes
@property (nonatomic) uint64_t diskHeadroomFloorBytes;        // default 256 MiB
@property (nonatomic) BOOL allowsCellularAccess;              // default NO

- (nullable instancetype)initWithDirectoryURL:(NSURL *)directoryURL
                                        error:(NSError **)error NS_DESIGNATED_INITIALIZER;
- (instancetype)init NS_UNAVAILABLE;

/// Returns the final file URL immediately when a previously verified copy exists
/// and still matches the spec size, otherwise nil.
- (nullable NSURL *)verifiedURLForSpec:(ODMFetchSpec *)spec;

/// Starts or resumes the fetch. Callbacks arrive on the main queue. Returns NO
/// and fills `error` when another fetch is already running.
- (BOOL)fetchSpec:(ODMFetchSpec *)spec
         progress:(nullable ODMProgressHandler)progress
       completion:(ODMCompletionHandler)completion
            error:(NSError **)error;

/// Stops the transfer and keeps the partial file so a later call resumes it.
- (void)cancel;

/// Deletes the partial file and its sidecar for this spec.
- (void)discardPartialForSpec:(ODMFetchSpec *)spec;

/// Computes the backoff for a 1 based attempt number. Exposed for tests.
+ (NSTimeInterval)backoffForAttempt:(NSUInteger)attempt
                               base:(NSTimeInterval)base
                                cap:(NSTimeInterval)cap
                         randomUnit:(double)randomUnit;

/// Parses a Retry-After header, either delta seconds or an HTTP date.
+ (NSTimeInterval)retryAfterSecondsFromHeader:(nullable NSString *)value now:(NSDate *)now;

@end

NS_ASSUME_NONNULL_END
