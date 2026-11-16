#import <Foundation/Foundation.h>

#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#ifndef __has_feature
#define __has_feature(x) 0
#endif

#if __has_feature(objc_arc)
#define PMA_AUTORELEASE(x) (x)
#define PMA_RELEASE(x) ((void)0)
#define PMA_SUPER_DEALLOC() ((void)0)
#else
#define PMA_AUTORELEASE(x) [(x) autorelease]
#define PMA_RELEASE(x) [(x) release]
#define PMA_SUPER_DEALLOC() [super dealloc]
#endif

#define PMA_VERSION "1.0.0"

#define PMA_EXIT_CLEAN 0
#define PMA_EXIT_FINDINGS 1
#define PMA_EXIT_USAGE 2

#define PMA_MH_MAGIC 0xfeedfaceu
#define PMA_MH_CIGAM 0xcefaedfeu
#define PMA_MH_MAGIC_64 0xfeedfacfu
#define PMA_MH_CIGAM_64 0xcffaedfeu
#define PMA_FAT_MAGIC 0xcafebabeu
#define PMA_FAT_MAGIC_64 0xcafebabfu
#define PMA_FAT_MAX_ARCHS 30u
#define PMA_MAX_NESTING 4

#define PMA_LC_SEGMENT 0x1u
#define PMA_LC_SYMTAB 0x2u
#define PMA_LC_SEGMENT_64 0x19u
#define PMA_LC_ENCRYPTION_INFO 0x21u
#define PMA_LC_ENCRYPTION_INFO_64 0x2cu
#define PMA_LC_DYLD_INFO 0x22u
#define PMA_LC_DYLD_INFO_ONLY 0x80000022u
#define PMA_LC_DYLD_CHAINED_FIXUPS 0x80000034u

#define PMA_MH_DSYM 0xau
#define PMA_S_ZEROFILL 0x1u
#define PMA_N_STAB 0xe0u
#define PMA_N_TYPE 0x0eu
#define PMA_N_EXT 0x01u
#define PMA_BIND_NON_WEAK_DEFINITION 0x8u

#define PMA_EVIDENCE_LIMIT 8

#define CAT_FILE "NSPrivacyAccessedAPICategoryFileTimestamp"
#define CAT_BOOT "NSPrivacyAccessedAPICategorySystemBootTime"
#define CAT_DISK "NSPrivacyAccessedAPICategoryDiskSpace"
#define CAT_KEYS "NSPrivacyAccessedAPICategoryActiveKeyboards"
#define CAT_DEFAULTS "NSPrivacyAccessedAPICategoryUserDefaults"

/* ------------------------------------------------------------------------ */
/* Required reason API catalog                                              */
/* ------------------------------------------------------------------------ */

typedef enum {
    PMAMatchSymbol = 0,
    PMAMatchClass,
    PMAMatchSelector,
    PMAMatchSwift
} PMAMatch;

typedef struct {
    const char *category;
    const char *alternate;
    PMAMatch match;
    const char *pattern;
    const char *label;
    int heuristic;
} PMARule;

static const PMARule kPMARules[] = {
    {CAT_FILE, NULL, PMAMatchSymbol, "stat", "stat()", 0},
    {CAT_FILE, NULL, PMAMatchSymbol, "fstat", "fstat()", 0},
    {CAT_FILE, NULL, PMAMatchSymbol, "lstat", "lstat()", 0},
    {CAT_FILE, NULL, PMAMatchSymbol, "fstatat", "fstatat()", 0},
    {CAT_FILE, NULL, PMAMatchSymbol, "stat64", "stat64()", 0},
    {CAT_FILE, NULL, PMAMatchSymbol, "fstat64", "fstat64()", 0},
    {CAT_FILE, NULL, PMAMatchSymbol, "lstat64", "lstat64()", 0},
    {CAT_FILE, CAT_DISK, PMAMatchSymbol, "getattrlist", "getattrlist()", 0},
    {CAT_FILE, CAT_DISK, PMAMatchSymbol, "getattrlistbulk", "getattrlistbulk()", 0},
    {CAT_FILE, CAT_DISK, PMAMatchSymbol, "fgetattrlist", "fgetattrlist()", 0},
    {CAT_FILE, CAT_DISK, PMAMatchSymbol, "getattrlistat", "getattrlistat()", 0},
    {CAT_FILE, NULL, PMAMatchSymbol, "NSFileCreationDate", "NSFileCreationDate", 0},
    {CAT_FILE, NULL, PMAMatchSymbol, "NSFileModificationDate", "NSFileModificationDate", 0},
    {CAT_FILE, NULL, PMAMatchSymbol, "NSURLContentModificationDateKey", "NSURLContentModificationDateKey", 0},
    {CAT_FILE, NULL, PMAMatchSymbol, "NSURLCreationDateKey", "NSURLCreationDateKey", 0},
    {CAT_FILE, NULL, PMAMatchSelector, "fileModificationDate", "-fileModificationDate", 0},
    {CAT_FILE, NULL, PMAMatchSwift, "10Foundation17URLResourceValuesV12creationDate", "URLResourceValues.creationDate", 0},
    {CAT_FILE, NULL, PMAMatchSwift, "10Foundation17URLResourceValuesV23contentModificationDate", "URLResourceValues.contentModificationDate", 0},
    {CAT_FILE, NULL, PMAMatchSelector, "fileCreationDate", "-fileCreationDate", 1},
    {CAT_FILE, NULL, PMAMatchSymbol, "kCFURLCreationDateKey", "kCFURLCreationDateKey", 1},
    {CAT_FILE, NULL, PMAMatchSymbol, "kCFURLContentModificationDateKey", "kCFURLContentModificationDateKey", 1},

    {CAT_BOOT, NULL, PMAMatchSelector, "systemUptime", "-[NSProcessInfo systemUptime]", 0},
    {CAT_BOOT, NULL, PMAMatchSymbol, "mach_absolute_time", "mach_absolute_time()", 0},

    {CAT_DISK, NULL, PMAMatchSymbol, "statfs", "statfs()", 0},
    {CAT_DISK, NULL, PMAMatchSymbol, "statvfs", "statvfs()", 0},
    {CAT_DISK, NULL, PMAMatchSymbol, "fstatfs", "fstatfs()", 0},
    {CAT_DISK, NULL, PMAMatchSymbol, "fstatvfs", "fstatvfs()", 0},
    {CAT_DISK, NULL, PMAMatchSymbol, "statfs64", "statfs64()", 0},
    {CAT_DISK, NULL, PMAMatchSymbol, "fstatfs64", "fstatfs64()", 0},
    {CAT_DISK, NULL, PMAMatchSymbol, "NSFileSystemFreeSize", "NSFileSystemFreeSize", 0},
    {CAT_DISK, NULL, PMAMatchSymbol, "NSFileSystemSize", "NSFileSystemSize", 0},
    {CAT_DISK, NULL, PMAMatchSymbol, "NSURLVolumeAvailableCapacityKey", "NSURLVolumeAvailableCapacityKey", 0},
    {CAT_DISK, NULL, PMAMatchSymbol, "NSURLVolumeAvailableCapacityForImportantUsageKey", "NSURLVolumeAvailableCapacityForImportantUsageKey", 0},
    {CAT_DISK, NULL, PMAMatchSymbol, "NSURLVolumeAvailableCapacityForOpportunisticUsageKey", "NSURLVolumeAvailableCapacityForOpportunisticUsageKey", 0},
    {CAT_DISK, NULL, PMAMatchSymbol, "NSURLVolumeTotalCapacityKey", "NSURLVolumeTotalCapacityKey", 0},
    {CAT_DISK, NULL, PMAMatchSwift, "10Foundation17URLResourceValuesV23volumeAvailableCapacity", "URLResourceValues.volumeAvailableCapacity", 0},
    {CAT_DISK, NULL, PMAMatchSwift, "10Foundation17URLResourceValuesV40volumeAvailableCapacityForImportantUsage", "URLResourceValues.volumeAvailableCapacityForImportantUsage", 0},
    {CAT_DISK, NULL, PMAMatchSwift, "10Foundation17URLResourceValuesV44volumeAvailableCapacityForOpportunisticUsage", "URLResourceValues.volumeAvailableCapacityForOpportunisticUsage", 0},
    {CAT_DISK, NULL, PMAMatchSwift, "10Foundation17URLResourceValuesV19volumeTotalCapacity", "URLResourceValues.volumeTotalCapacity", 0},
    {CAT_DISK, NULL, PMAMatchSymbol, "kCFURLVolumeAvailableCapacityKey", "kCFURLVolumeAvailableCapacityKey", 1},
    {CAT_DISK, NULL, PMAMatchSymbol, "kCFURLVolumeTotalCapacityKey", "kCFURLVolumeTotalCapacityKey", 1},

    {CAT_KEYS, NULL, PMAMatchSelector, "activeInputModes", "+[UITextInputMode activeInputModes]", 0},

    {CAT_DEFAULTS, NULL, PMAMatchClass, "NSUserDefaults", "NSUserDefaults", 0},
    {CAT_DEFAULTS, NULL, PMAMatchSelector, "standardUserDefaults", "+standardUserDefaults", 1},
    {CAT_DEFAULTS, NULL, PMAMatchSymbol, "CFPreferencesCopyAppValue", "CFPreferencesCopyAppValue()", 1},
    {CAT_DEFAULTS, NULL, PMAMatchSymbol, "CFPreferencesSetAppValue", "CFPreferencesSetAppValue()", 1},
    {CAT_DEFAULTS, NULL, PMAMatchSymbol, "CFPreferencesCopyValue", "CFPreferencesCopyValue()", 1},
    {CAT_DEFAULTS, NULL, PMAMatchSymbol, "CFPreferencesSetValue", "CFPreferencesSetValue()", 1},
    {CAT_DEFAULTS, NULL, PMAMatchSymbol, "CFPreferencesGetAppBooleanValue", "CFPreferencesGetAppBooleanValue()", 1},
    {CAT_DEFAULTS, NULL, PMAMatchSymbol, "CFPreferencesGetAppIntegerValue", "CFPreferencesGetAppIntegerValue()", 1},
    {CAT_DEFAULTS, NULL, PMAMatchSymbol, "CFPreferencesAppSynchronize", "CFPreferencesAppSynchronize()", 1},
    {CAT_DEFAULTS, NULL, PMAMatchSwift, "7SwiftUI10AppStorageV", "SwiftUI @AppStorage", 1},
};

#define PMA_RULE_COUNT (sizeof(kPMARules) / sizeof(kPMARules[0]))

typedef struct {
    const char *category;
    const char *code;
    const char *summary;
} PMAReason;

static const PMAReason kPMAReasons[] = {
    {CAT_FILE, "DDA9.1", "Show file timestamps to the person using the device; must not leave the device"},
    {CAT_FILE, "C617.1", "Read timestamps, size or metadata of files inside the app, app group or CloudKit container"},
    {CAT_FILE, "3B52.1", "Read timestamps of files or folders the user explicitly granted access to"},
    {CAT_FILE, "0A2A.1", "Third party SDK wrapper around timestamp APIs, used only when the app calls the wrapper"},
    {CAT_BOOT, "35F9.1", "Measure elapsed time between events inside the app or drive timers"},
    {CAT_BOOT, "8FFB.1", "Compute absolute timestamps for events inside the app, for example UIKit or AVFAudio events"},
    {CAT_BOOT, "3D61.1", "Include boot time in an optional bug report the user chooses to send"},
    {CAT_DISK, "85F4.1", "Show disk space to the person using the device"},
    {CAT_DISK, "E174.1", "Check there is room to write files, or free space when it runs low"},
    {CAT_DISK, "7D9E.1", "Include disk space in an optional bug report the user chooses to send"},
    {CAT_DISK, "B728.1", "Health research app warning participants that low disk space affects data collection"},
    {CAT_KEYS, "3EC4.1", "Custom keyboard app checking which keyboards are active"},
    {CAT_KEYS, "54BD.1", "Adjust the app UI based on the active keyboards"},
    {CAT_DEFAULTS, "CA92.1", "Read and write data only this app can see"},
    {CAT_DEFAULTS, "1C8F.1", "Read and write data shared by apps, extensions and App Clips in the same App Group"},
    {CAT_DEFAULTS, "C56D.1", "Third party SDK wrapper around user defaults, used only when the app calls the wrapper"},
    {CAT_DEFAULTS, "AC6B.1", "Read the managed app configuration key or write the managed feedback key (MDM)"},
};

#define PMA_REASON_COUNT (sizeof(kPMAReasons) / sizeof(kPMAReasons[0]))

static const char *kPMACategories[] = {CAT_FILE, CAT_BOOT, CAT_DISK, CAT_KEYS, CAT_DEFAULTS};
#define PMA_CATEGORY_COUNT (sizeof(kPMACategories) / sizeof(kPMACategories[0]))

/* SDKs Apple names as commonly used: embedded binary copies must ship their own manifest. */
static const char *kPMACommonSDKs[] = {
    "Abseil", "AFNetworking", "Alamofire", "AppAuth", "BoringSSL", "openssl_grpc", "Capacitor", "Charts",
    "connectivity_plus", "Cordova", "device_info_plus", "DKImagePickerController", "DKPhotoGallery",
    "FBAEMKit", "FBLPromises", "FBSDKCoreKit", "FBSDKCoreKit_Basics", "FBSDKLoginKit", "FBSDKShareKit",
    "file_picker", "FirebaseABTesting", "FirebaseAuth", "FirebaseCore", "FirebaseCoreDiagnostics",
    "FirebaseCoreExtension", "FirebaseCoreInternal", "FirebaseCrashlytics", "FirebaseDynamicLinks",
    "FirebaseFirestore", "FirebaseInstallations", "FirebaseMessaging", "FirebaseRemoteConfig", "Flutter",
    "flutter_inappwebview", "flutter_local_notifications", "fluttertoast", "FMDB", "geolocator_apple",
    "GoogleDataTransport", "GoogleSignIn", "GoogleToolboxForMac", "GoogleUtilities", "grpcpp", "GTMAppAuth",
    "GTMSessionFetcher", "hermes", "image_picker_ios", "IQKeyboardManager", "IQKeyboardManagerSwift",
    "Kingfisher", "leveldb", "Lottie", "MBProgressHUD", "nanopb", "OneSignal", "OneSignalCore",
    "OneSignalExtension", "OneSignalOutcomes", "OpenSSL", "OrderedSet", "package_info", "package_info_plus",
    "path_provider", "path_provider_ios", "Promises", "Protobuf", "Reachability", "RealmSwift", "RxCocoa",
    "RxRelay", "RxSwift", "SDWebImage", "share_plus", "shared_preferences_ios", "SnapKit", "sqflite",
    "Starscream", "SVProgressHUD", "SwiftyGif", "SwiftyJSON", "Toast", "UnityFramework", "url_launcher",
    "url_launcher_ios", "video_player_avfoundation", "wakelock", "webview_flutter_wkwebview",
};

#define PMA_SDK_COUNT (sizeof(kPMACommonSDKs) / sizeof(kPMACommonSDKs[0]))

static const char *kPMADataTypes[] = {
    "Name", "EmailAddress", "PhoneNumber", "PhysicalAddress", "OtherUserContactInfo", "Health", "Fitness",
    "PaymentInfo", "CreditInfo", "OtherFinancialInfo", "PreciseLocation", "CoarseLocation", "SensitiveInfo",
    "Contacts", "EmailsOrTextMessages", "PhotosorVideos", "AudioData", "GameplayContent", "CustomerSupport",
    "OtherUserContent", "BrowsingHistory", "SearchHistory", "UserID", "DeviceID", "PurchaseHistory",
    "ProductInteraction", "AdvertisingData", "OtherUsageData", "CrashData", "PerformanceData",
    "OtherDiagnosticData", "EnvironmentScanning", "Hands", "Head", "OtherDataTypes",
};

#define PMA_DATA_TYPE_COUNT (sizeof(kPMADataTypes) / sizeof(kPMADataTypes[0]))

static const char *kPMAPurposes[] = {
    "ThirdPartyAdvertising", "DeveloperAdvertising", "Analytics", "ProductPersonalization",
    "AppFunctionality", "Other",
};

#define PMA_PURPOSE_COUNT (sizeof(kPMAPurposes) / sizeof(kPMAPurposes[0]))

/* ------------------------------------------------------------------------ */
/* Mach-O, fat and ar reader. Pure C, bounds checked, never trusts offsets.  */
/* ------------------------------------------------------------------------ */

typedef enum {
    PMAKindImport = 1,
    PMAKindSelector,
    PMAKindArch,
    PMAKindDiagnostic
} PMANameKind;

typedef void (*PMASink)(void *ctx, PMANameKind kind, const char *text, size_t len);

typedef struct {
    PMASink sink;
    void *ctx;
    int depth;
    const char *member;
} PMAScanState;

static int pma_scan_buffer(PMAScanState *st, const uint8_t *b, size_t n);

static uint32_t pma_le32(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

static uint32_t pma_be32(const uint8_t *p) {
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) | ((uint32_t)p[2] << 8) | (uint32_t)p[3];
}

static uint64_t pma_le64(const uint8_t *p) {
    return (uint64_t)pma_le32(p) | ((uint64_t)pma_le32(p + 4) << 32);
}

static uint64_t pma_be64(const uint8_t *p) {
    return ((uint64_t)pma_be32(p) << 32) | (uint64_t)pma_be32(p + 4);
}

static uint32_t pma_rd32(const uint8_t *p, int be) { return be ? pma_be32(p) : pma_le32(p); }
static uint64_t pma_rd64(const uint8_t *p, int be) { return be ? pma_be64(p) : pma_le64(p); }

static int pma_fits(size_t total, uint64_t off, uint64_t len) {
    return off <= (uint64_t)total && len <= (uint64_t)total - off;
}

static void pma_emit(PMAScanState *st, PMANameKind kind, const char *text, size_t len) {
    st->sink(st->ctx, kind, text, len);
}

static void pma_diag(PMAScanState *st, const char *fmt, ...) {
    char msg[384];
    char full[512];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, sizeof msg, fmt, ap);
    va_end(ap);
    if (st->member) {
        snprintf(full, sizeof full, "archive member %s: %s", st->member, msg);
    } else {
        snprintf(full, sizeof full, "%s", msg);
    }
    pma_emit(st, PMAKindDiagnostic, full, strlen(full));
}

static int pma_uleb(const uint8_t **p, const uint8_t *end, uint64_t *out) {
    uint64_t result = 0;
    unsigned shift = 0;
    while (*p < end) {
        uint8_t byte = *(*p)++;
        if (shift < 64) result |= (uint64_t)(byte & 0x7f) << shift;
        shift += 7;
        if (!(byte & 0x80)) {
            *out = result;
            return 1;
        }
        if (shift > 70) return 0;
    }
    return 0;
}

static const char *pma_arch_name(uint32_t cpu, uint32_t sub) {
    sub &= 0x00ffffffu;
    switch (cpu) {
        case 0x01000007u: return sub == 8 ? "x86_64h" : "x86_64";
        case 0x7u: return "i386";
        case 0x0100000cu: return sub == 2 ? "arm64e" : "arm64";
        case 0x0200000cu: return "arm64_32";
        case 0xcu: return sub == 11 ? "armv7s" : (sub == 12 ? "armv7k" : "armv7");
        case 0x01000012u: return "ppc64";
        case 0x12u: return "ppc";
        default: return "unknown";
    }
}

static void pma_emit_cstrings(PMAScanState *st, const uint8_t *b, size_t n, uint64_t off, uint64_t size) {
    if (!pma_fits(n, off, size)) {
        pma_diag(st, "__objc_methname section lies outside the file; selector checks skipped");
        return;
    }
    const char *p = (const char *)(b + off);
    const char *end = p + size;
    while (p < end) {
        const char *z = memchr(p, 0, (size_t)(end - p));
        size_t len = z ? (size_t)(z - p) : (size_t)(end - p);
        if (len) pma_emit(st, PMAKindSelector, p, len);
        p += len + 1;
    }
}

static void pma_scan_symtab(PMAScanState *st, const uint8_t *b, size_t n, int be, int is64,
                            uint32_t symoff, uint32_t nsyms, uint32_t stroff, uint32_t strsize) {
    size_t esz = is64 ? 16 : 12;
    if (!pma_fits(n, stroff, strsize)) {
        pma_diag(st, "string table lies outside the file; symbol imports skipped");
        return;
    }
    if (!pma_fits(n, symoff, (uint64_t)nsyms * esz)) {
        pma_diag(st, "symbol table lies outside the file; symbol imports skipped");
        return;
    }
    const char *strtab = (const char *)(b + stroff);
    for (uint32_t i = 0; i < nsyms; i++) {
        const uint8_t *e = b + symoff + (size_t)i * esz;
        uint32_t strx = pma_rd32(e, be);
        uint8_t type = e[4];
        uint64_t value = is64 ? pma_rd64(e + 8, be) : pma_rd32(e + 8, be);
        if (type & PMA_N_STAB) continue;
        if ((type & PMA_N_TYPE) != 0) continue;
        if (!(type & PMA_N_EXT)) continue;
        if (value != 0) continue;
        if (strx == 0 || strx >= strsize) continue;
        size_t max = strsize - strx;
        size_t len = strnlen(strtab + strx, max);
        if (len == 0 || len == max) continue;
        pma_emit(st, PMAKindImport, strtab + strx, len);
    }
}

static void pma_scan_bind(PMAScanState *st, const uint8_t *b, size_t n, uint32_t off, uint32_t size,
                          const char *label) {
    if (size == 0) return;
    if (!pma_fits(n, off, size)) {
        pma_diag(st, "%s bind info lies outside the file; skipped", label);
        return;
    }
    const uint8_t *p = b + off;
    const uint8_t *end = p + size;
    uint64_t scratch = 0;
    while (p < end) {
        uint8_t byte = *p++;
        uint8_t op = byte & 0xf0;
        uint8_t imm = byte & 0x0f;
        switch (op) {
            case 0x00: case 0x10: case 0x30: case 0x50: case 0x90: case 0xb0:
                break;
            case 0x20: case 0x60: case 0x70: case 0x80: case 0xa0:
                if (!pma_uleb(&p, end, &scratch)) goto malformed;
                break;
            case 0xc0:
                if (!pma_uleb(&p, end, &scratch) || !pma_uleb(&p, end, &scratch)) goto malformed;
                break;
            case 0x40: {
                const uint8_t *s = p;
                while (p < end && *p) p++;
                if (p >= end) goto malformed;
                if (!(imm & PMA_BIND_NON_WEAK_DEFINITION) && p > s) {
                    pma_emit(st, PMAKindImport, (const char *)s, (size_t)(p - s));
                }
                p++;
                break;
            }
            case 0xd0:
                if (imm == 0x00) {
                    if (!pma_uleb(&p, end, &scratch)) goto malformed;
                } else if (imm != 0x01) {
                    goto malformed;
                }
                break;
            default:
                goto malformed;
        }
    }
    return;
malformed:
    pma_diag(st, "%s bind opcodes are malformed near byte %lu; the rest of that stream was skipped", label,
             (unsigned long)(p - (b + off)));
}

static void pma_scan_chained(PMAScanState *st, const uint8_t *b, size_t n, int be, uint32_t dataoff,
                             uint32_t datasize) {
    if (!pma_fits(n, dataoff, datasize) || datasize < 28) {
        pma_diag(st, "chained fixups header is truncated; imports from it were skipped");
        return;
    }
    const uint8_t *h = b + dataoff;
    uint32_t importsOffset = pma_rd32(h + 8, be);
    uint32_t symbolsOffset = pma_rd32(h + 12, be);
    uint32_t count = pma_rd32(h + 16, be);
    uint32_t format = pma_rd32(h + 20, be);
    uint32_t symbolsFormat = pma_rd32(h + 24, be);
    if (symbolsFormat != 0) {
        pma_diag(st, "chained fixups symbol names are compressed (format %u); imports from it were skipped",
                 symbolsFormat);
        return;
    }
    size_t esz = format == 1 ? 4 : (format == 2 ? 8 : (format == 3 ? 16 : 0));
    if (esz == 0) {
        pma_diag(st, "unknown chained import format %u; imports from it were skipped", format);
        return;
    }
    if (!pma_fits(datasize, importsOffset, (uint64_t)count * esz) || symbolsOffset > datasize) {
        pma_diag(st, "chained import table lies outside its load command; skipped");
        return;
    }
    const char *syms = (const char *)(h + symbolsOffset);
    size_t symsLen = datasize - symbolsOffset;
    for (uint32_t i = 0; i < count; i++) {
        const uint8_t *e = h + importsOffset + (size_t)i * esz;
        uint64_t nameOffset = format == 3 ? (pma_rd64(e, be) >> 32) : (uint64_t)(pma_rd32(e, be) >> 9);
        if (nameOffset >= symsLen) continue;
        size_t max = symsLen - (size_t)nameOffset;
        size_t len = strnlen(syms + nameOffset, max);
        if (len == 0 || len == max) continue;
        pma_emit(st, PMAKindImport, syms + nameOffset, len);
    }
}

static void pma_scan_thin(PMAScanState *st, const uint8_t *b, size_t n) {
    if (n < 28) {
        pma_diag(st, "Mach-O header is truncated");
        return;
    }
    uint32_t magic = pma_le32(b);
    int be = (magic == PMA_MH_CIGAM || magic == PMA_MH_CIGAM_64);
    int is64 = (magic == PMA_MH_MAGIC_64 || magic == PMA_MH_CIGAM_64);
    size_t hdr = is64 ? 32 : 28;
    if (n < hdr) {
        pma_diag(st, "Mach-O header is truncated");
        return;
    }
    uint32_t cpu = pma_rd32(b + 4, be);
    uint32_t sub = pma_rd32(b + 8, be);
    uint32_t filetype = pma_rd32(b + 12, be);
    uint32_t ncmds = pma_rd32(b + 16, be);
    uint32_t sizeofcmds = pma_rd32(b + 20, be);
    if (filetype == PMA_MH_DSYM) return;

    const char *arch = pma_arch_name(cpu, sub);
    pma_emit(st, PMAKindArch, arch, strlen(arch));

    if (!pma_fits(n, hdr, sizeofcmds)) {
        pma_diag(st, "load commands run past the end of the %s slice", arch);
        return;
    }

    uint64_t methOff[4] = {0}, methSize[4] = {0};
    int methCount = 0;
    uint64_t cryptOff = 0, cryptSize = 0;
    uint32_t cryptId = 0;

    const uint8_t *lc = b + hdr;
    const uint8_t *lcEnd = lc + sizeofcmds;
    for (uint32_t i = 0; i < ncmds; i++) {
        if (lcEnd - lc < 8) {
            pma_diag(st, "load command %u of the %s slice is truncated", i, arch);
            break;
        }
        uint32_t cmd = pma_rd32(lc, be);
        uint32_t cmdsize = pma_rd32(lc + 4, be);
        if (cmdsize < 8 || cmdsize > (uint64_t)(lcEnd - lc)) {
            pma_diag(st, "load command %u of the %s slice has an invalid size %u", i, arch, cmdsize);
            break;
        }
        if (cmd == PMA_LC_SEGMENT_64 && cmdsize >= 72) {
            uint32_t nsects = pma_rd32(lc + 64, be);
            if (72 + (uint64_t)nsects * 80 <= cmdsize) {
                for (uint32_t k = 0; k < nsects; k++) {
                    const uint8_t *s = lc + 72 + (size_t)k * 80;
                    uint32_t flags = pma_rd32(s + 64, be);
                    if (strncmp((const char *)s, "__objc_methname", 16) == 0 && (flags & 0xff) != PMA_S_ZEROFILL &&
                        methCount < 4) {
                        methSize[methCount] = pma_rd64(s + 40, be);
                        methOff[methCount] = pma_rd32(s + 48, be);
                        methCount++;
                    }
                }
            }
        } else if (cmd == PMA_LC_SEGMENT && cmdsize >= 56) {
            uint32_t nsects = pma_rd32(lc + 48, be);
            if (56 + (uint64_t)nsects * 68 <= cmdsize) {
                for (uint32_t k = 0; k < nsects; k++) {
                    const uint8_t *s = lc + 56 + (size_t)k * 68;
                    uint32_t flags = pma_rd32(s + 56, be);
                    if (strncmp((const char *)s, "__objc_methname", 16) == 0 && (flags & 0xff) != PMA_S_ZEROFILL &&
                        methCount < 4) {
                        methSize[methCount] = pma_rd32(s + 36, be);
                        methOff[methCount] = pma_rd32(s + 40, be);
                        methCount++;
                    }
                }
            }
        } else if (cmd == PMA_LC_SYMTAB && cmdsize >= 24) {
            pma_scan_symtab(st, b, n, be, is64, pma_rd32(lc + 8, be), pma_rd32(lc + 12, be),
                            pma_rd32(lc + 16, be), pma_rd32(lc + 20, be));
        } else if ((cmd == PMA_LC_DYLD_INFO || cmd == PMA_LC_DYLD_INFO_ONLY) && cmdsize >= 48) {
            pma_scan_bind(st, b, n, pma_rd32(lc + 16, be), pma_rd32(lc + 20, be), "regular");
            pma_scan_bind(st, b, n, pma_rd32(lc + 24, be), pma_rd32(lc + 28, be), "weak");
            pma_scan_bind(st, b, n, pma_rd32(lc + 32, be), pma_rd32(lc + 36, be), "lazy");
        } else if (cmd == PMA_LC_DYLD_CHAINED_FIXUPS && cmdsize >= 16) {
            pma_scan_chained(st, b, n, be, pma_rd32(lc + 8, be), pma_rd32(lc + 12, be));
        } else if ((cmd == PMA_LC_ENCRYPTION_INFO || cmd == PMA_LC_ENCRYPTION_INFO_64) && cmdsize >= 20) {
            cryptOff = pma_rd32(lc + 8, be);
            cryptSize = pma_rd32(lc + 12, be);
            cryptId = pma_rd32(lc + 16, be);
        }
        lc += cmdsize;
    }

    for (int m = 0; m < methCount; m++) {
        int overlaps = cryptId != 0 && methOff[m] < cryptOff + cryptSize && cryptOff < methOff[m] + methSize[m];
        if (overlaps) {
            pma_diag(st, "%s slice is FairPlay encrypted; selector checks skipped, scan the archive build instead",
                     arch);
            continue;
        }
        pma_emit_cstrings(st, b, n, methOff[m], methSize[m]);
    }
}

static void pma_scan_fat(PMAScanState *st, const uint8_t *b, size_t n, int is64, uint32_t nfat) {
    size_t esz = is64 ? 32 : 20;
    if (!pma_fits(n, 8, (uint64_t)nfat * esz)) {
        pma_diag(st, "fat header is truncated");
        return;
    }
    for (uint32_t i = 0; i < nfat; i++) {
        const uint8_t *a = b + 8 + (size_t)i * esz;
        uint64_t off = is64 ? pma_be64(a + 8) : pma_be32(a + 8);
        uint64_t size = is64 ? pma_be64(a + 16) : pma_be32(a + 12);
        if (!pma_fits(n, off, size)) {
            pma_diag(st, "fat slice %u lies outside the file", i);
            continue;
        }
        if (!pma_scan_buffer(st, b + off, (size_t)size)) {
            pma_diag(st, "fat slice %u is not a Mach-O image or archive", i);
        }
    }
}

static int pma_is_bitcode(const uint8_t *p, size_t n) {
    if (n < 4) return 0;
    if (p[0] == 'B' && p[1] == 'C' && p[2] == 0xc0 && p[3] == 0xde) return 1;
    return pma_le32(p) == 0x0b17c0deu;
}

static void pma_scan_archive(PMAScanState *st, const uint8_t *b, size_t n) {
    size_t off = 8;
    const char *outer = st->member;
    while (off + 60 <= n) {
        const uint8_t *h = b + off;
        if (h[58] != '`' || h[59] != '\n') {
            pma_diag(st, "archive member header at byte %lu is malformed; rest of archive skipped",
                     (unsigned long)off);
            return;
        }
        char sizeText[11];
        memcpy(sizeText, h + 48, 10);
        sizeText[10] = 0;
        uint64_t size = strtoull(sizeText, NULL, 10);
        size_t dataOff = off + 60;
        if (!pma_fits(n, dataOff, size)) {
            pma_diag(st, "archive member at byte %lu is truncated", (unsigned long)off);
            return;
        }
        const uint8_t *data = b + dataOff;
        uint64_t dataLen = size;
        char name[256];
        if (memcmp(h, "#1/", 3) == 0) {
            char lenText[14];
            memcpy(lenText, h + 3, 13);
            lenText[13] = 0;
            uint64_t nameLen = strtoull(lenText, NULL, 10);
            if (nameLen > size) {
                pma_diag(st, "archive member at byte %lu has a name longer than its data", (unsigned long)off);
                return;
            }
            size_t copy = nameLen < sizeof name - 1 ? (size_t)nameLen : sizeof name - 1;
            memcpy(name, data, copy);
            name[copy] = 0;
            data += nameLen;
            dataLen -= nameLen;
        } else {
            memcpy(name, h, 16);
            name[16] = 0;
            for (int k = 15; k >= 0 && (name[k] == ' ' || name[k] == '/'); k--) name[k] = 0;
        }
        int bookkeeping = strncmp(name, "__.SYMDEF", 9) == 0 || name[0] == 0 || strcmp(name, "/SYM64") == 0;
        if (!bookkeeping) {
            st->member = name;
            if (pma_is_bitcode(data, (size_t)dataLen)) {
                pma_diag(st, "LLVM bitcode (LTO) cannot be read; ship machine code in the static library");
            } else {
                pma_scan_buffer(st, data, (size_t)dataLen);
            }
            st->member = outer;
        }
        off = dataOff + (size_t)size;
        if (off & 1) off++;
    }
}

static int pma_scan_buffer(PMAScanState *st, const uint8_t *b, size_t n) {
    if (n < 8) return 0;
    if (st->depth >= PMA_MAX_NESTING) {
        pma_diag(st, "container nesting is deeper than %d levels; inner content skipped", PMA_MAX_NESTING);
        return 1;
    }
    int recognized = 0;
    st->depth++;
    uint32_t big = pma_be32(b);
    uint32_t little = pma_le32(b);
    if (memcmp(b, "!<arch>\n", 8) == 0) {
        pma_scan_archive(st, b, n);
        recognized = 1;
    } else if (big == PMA_FAT_MAGIC || big == PMA_FAT_MAGIC_64) {
        uint32_t nfat = pma_be32(b + 4);
        if (nfat >= 1 && nfat <= PMA_FAT_MAX_ARCHS) {
            pma_scan_fat(st, b, n, big == PMA_FAT_MAGIC_64, nfat);
            recognized = 1;
        }
    } else if (little == PMA_MH_MAGIC || little == PMA_MH_CIGAM || little == PMA_MH_MAGIC_64 ||
               little == PMA_MH_CIGAM_64) {
        pma_scan_thin(st, b, n);
        recognized = 1;
    }
    st->depth--;
    return recognized;
}

static int pma_sniff_magic(const uint8_t *b, size_t n) {
    if (n < 8) return 0;
    if (memcmp(b, "!<arch>\n", 8) == 0) return 1;
    uint32_t big = pma_be32(b);
    uint32_t little = pma_le32(b);
    if (big == PMA_FAT_MAGIC || big == PMA_FAT_MAGIC_64) {
        uint32_t nfat = pma_be32(b + 4);
        return nfat >= 1 && nfat <= PMA_FAT_MAX_ARCHS;
    }
    return little == PMA_MH_MAGIC || little == PMA_MH_CIGAM || little == PMA_MH_MAGIC_64 ||
           little == PMA_MH_CIGAM_64;
}

static const void *pma_memmem(const void *hay, size_t hayLen, const char *needle, size_t needleLen) {
    if (needleLen == 0 || hayLen < needleLen) return NULL;
    const char *h = hay;
    const char *last = h + hayLen - needleLen;
    for (const char *p = h; p <= last; p++) {
        p = memchr(p, needle[0], (size_t)(last - p) + 1);
        if (!p) return NULL;
        if (memcmp(p, needle, needleLen) == 0) return p;
    }
    return NULL;
}

/* Calls hit(ctx, ruleIndex) for each catalog rule the raw name matches. */
static void pma_match(PMANameKind kind, const char *t, size_t n, void (*hit)(void *, size_t), void *ctx) {
    if (kind == PMAKindSelector) {
        for (size_t r = 0; r < PMA_RULE_COUNT; r++) {
            const PMARule *rule = &kPMARules[r];
            if (rule->match == PMAMatchSelector && strlen(rule->pattern) == n && memcmp(rule->pattern, t, n) == 0) {
                hit(ctx, r);
            }
        }
        return;
    }
    if (kind != PMAKindImport) return;

    static const char classPrefix[] = "_OBJC_CLASS_$_";
    size_t classPrefixLen = sizeof classPrefix - 1;
    if (n > classPrefixLen && memcmp(t, classPrefix, classPrefixLen) == 0) {
        const char *cls = t + classPrefixLen;
        size_t clsLen = n - classPrefixLen;
        for (size_t r = 0; r < PMA_RULE_COUNT; r++) {
            const PMARule *rule = &kPMARules[r];
            if (rule->match == PMAMatchClass && strlen(rule->pattern) == clsLen &&
                memcmp(rule->pattern, cls, clsLen) == 0) {
                hit(ctx, r);
            }
        }
        return;
    }

    const char *s = t;
    size_t len = n;
    if (len && *s == '_') {
        s++;
        len--;
    }
    if (len > 2 && s[0] == '$' && (s[1] == 's' || s[1] == 'S')) {
        for (size_t r = 0; r < PMA_RULE_COUNT; r++) {
            const PMARule *rule = &kPMARules[r];
            if (rule->match == PMAMatchSwift && pma_memmem(s, len, rule->pattern, strlen(rule->pattern))) {
                hit(ctx, r);
            }
        }
        return;
    }
    const char *dollar = memchr(s, '$', len);
    if (dollar) len = (size_t)(dollar - s);
    for (size_t r = 0; r < PMA_RULE_COUNT; r++) {
        const PMARule *rule = &kPMARules[r];
        if (rule->match == PMAMatchSymbol && strlen(rule->pattern) == len && memcmp(rule->pattern, s, len) == 0) {
            hit(ctx, r);
        }
    }
}

/* ------------------------------------------------------------------------ */
/* Catalog helpers                                                          */
/* ------------------------------------------------------------------------ */

static NSString *PMAStr(const char *s) {
    return s ? [NSString stringWithUTF8String:s] : nil;
}

static NSString *PMAShortCategory(NSString *category) {
    NSString *prefix = @"NSPrivacyAccessedAPICategory";
    return [category hasPrefix:prefix] ? [category substringFromIndex:[prefix length]] : category;
}

static BOOL PMAIsKnownCategory(NSString *category) {
    for (size_t i = 0; i < PMA_CATEGORY_COUNT; i++) {
        if ([category isEqualToString:PMAStr(kPMACategories[i])]) return YES;
    }
    return NO;
}

static NSString *PMACategoryForReason(NSString *code) {
    for (size_t i = 0; i < PMA_REASON_COUNT; i++) {
        if ([code isEqualToString:PMAStr(kPMAReasons[i].code)]) return PMAStr(kPMAReasons[i].category);
    }
    return nil;
}

static BOOL PMAListContains(const char **list, size_t count, NSString *value) {
    for (size_t i = 0; i < count; i++) {
        if ([value isEqualToString:PMAStr(list[i])]) return YES;
    }
    return NO;
}

static NSInteger PMASeverityRank(NSString *severity) {
    if ([severity isEqualToString:@"error"]) return 0;
    if ([severity isEqualToString:@"warning"]) return 1;
    return 2;
}

static NSInteger PMACompareFindings(id a, id b, void *ctx) {
    (void)ctx;
    NSInteger ra = PMASeverityRank([a objectForKey:@"severity"]);
    NSInteger rb = PMASeverityRank([b objectForKey:@"severity"]);
    if (ra != rb) return ra < rb ? NSOrderedAscending : NSOrderedDescending;
    NSComparisonResult byPath = [(NSString *)[a objectForKey:@"path"] compare:[b objectForKey:@"path"]];
    if (byPath != NSOrderedSame) return byPath;
    NSComparisonResult byCode = [(NSString *)[a objectForKey:@"code"] compare:[b objectForKey:@"code"]];
    if (byCode != NSOrderedSame) return byCode;
    return [(NSString *)[a objectForKey:@"message"] compare:[b objectForKey:@"message"]];
}

static NSInteger PMACompareStrings(id a, id b, void *ctx) {
    (void)ctx;
    return [(NSString *)a compare:(NSString *)b];
}

static NSArray *PMASorted(id collection) {
    NSArray *items = [collection isKindOfClass:[NSArray class]] ? collection : [collection allObjects];
    return [items sortedArrayUsingFunction:PMACompareStrings context:NULL];
}

/* ------------------------------------------------------------------------ */
/* Deterministic JSON writer: sorted keys so CI diffs stay stable.          */
/* ------------------------------------------------------------------------ */

static void PMAJSONAppend(NSMutableString *out, id obj, NSUInteger indent);

static void PMAJSONIndent(NSMutableString *out, NSUInteger indent) {
    for (NSUInteger i = 0; i < indent; i++) [out appendString:@"  "];
}

static void PMAJSONString(NSMutableString *out, NSString *s) {
    [out appendString:@"\""];
    NSUInteger len = [s length];
    for (NSUInteger i = 0; i < len; i++) {
        unichar c = [s characterAtIndex:i];
        switch (c) {
            case '"': [out appendString:@"\\\""]; break;
            case '\\': [out appendString:@"\\\\"]; break;
            case '\n': [out appendString:@"\\n"]; break;
            case '\r': [out appendString:@"\\r"]; break;
            case '\t': [out appendString:@"\\t"]; break;
            default:
                if (c < 0x20) {
                    [out appendFormat:@"\\u%04x", (unsigned)c];
                } else {
                    [out appendFormat:@"%C", c];
                }
        }
    }
    [out appendString:@"\""];
}

static void PMAJSONAppend(NSMutableString *out, id obj, NSUInteger indent) {
    if (obj == nil || obj == [NSNull null]) {
        [out appendString:@"null"];
    } else if ([obj isKindOfClass:[NSString class]]) {
        PMAJSONString(out, obj);
    } else if ([obj isKindOfClass:[NSNumber class]]) {
        [out appendFormat:@"%lld", [obj longLongValue]];
    } else if ([obj isKindOfClass:[NSDictionary class]]) {
        NSArray *keys = PMASorted([obj allKeys]);
        if ([keys count] == 0) {
            [out appendString:@"{}"];
            return;
        }
        [out appendString:@"{\n"];
        for (NSUInteger i = 0; i < [keys count]; i++) {
            NSString *key = [keys objectAtIndex:i];
            PMAJSONIndent(out, indent + 1);
            PMAJSONString(out, key);
            [out appendString:@": "];
            PMAJSONAppend(out, [obj objectForKey:key], indent + 1);
            [out appendString:i + 1 < [keys count] ? @",\n" : @"\n"];
        }
        PMAJSONIndent(out, indent);
        [out appendString:@"}"];
    } else if ([obj isKindOfClass:[NSArray class]]) {
        if ([obj count] == 0) {
            [out appendString:@"[]"];
            return;
        }
        [out appendString:@"[\n"];
        for (NSUInteger i = 0; i < [obj count]; i++) {
            PMAJSONIndent(out, indent + 1);
            PMAJSONAppend(out, [obj objectAtIndex:i], indent + 1);
            [out appendString:i + 1 < [obj count] ? @",\n" : @"\n"];
        }
        PMAJSONIndent(out, indent);
        [out appendString:@"]"];
    } else {
        PMAJSONString(out, [obj description]);
    }
}

static void PMAPrint(NSString *s) {
    fputs([s UTF8String], stdout);
}

static void PMAPrintErr(NSString *s) {
    fputs([s UTF8String], stderr);
}

/* ------------------------------------------------------------------------ */
/* Model                                                                    */
/* ------------------------------------------------------------------------ */

@interface PMABinaryScan : NSObject {
    NSString *_relativePath;
    NSMutableArray *_archs;
    NSMutableArray *_diagnostics;
    NSMutableIndexSet *_hits;
    unsigned long long _importCount;
    unsigned long long _selectorCount;
}
@property (nonatomic, copy) NSString *relativePath;
@property (nonatomic, strong) NSMutableArray *archs;
@property (nonatomic, strong) NSMutableArray *diagnostics;
@property (nonatomic, strong) NSMutableIndexSet *hits;
@property (nonatomic, assign) unsigned long long importCount;
@property (nonatomic, assign) unsigned long long selectorCount;
- (void)consume:(PMANameKind)kind bytes:(const char *)bytes length:(size_t)length;
- (NSString *)archSummary;
@end

static void PMARecordHit(void *ctx, size_t ruleIndex) {
    PMABinaryScan *scan = (__bridge PMABinaryScan *)ctx;
    [[scan hits] addIndex:ruleIndex];
}

static void PMACollect(void *ctx, PMANameKind kind, const char *text, size_t len) {
    PMABinaryScan *scan = (__bridge PMABinaryScan *)ctx;
    [scan consume:kind bytes:text length:len];
}

@implementation PMABinaryScan
@synthesize relativePath = _relativePath;
@synthesize archs = _archs;
@synthesize diagnostics = _diagnostics;
@synthesize hits = _hits;
@synthesize importCount = _importCount;
@synthesize selectorCount = _selectorCount;

- (instancetype)init {
    if ((self = [super init])) {
        _archs = [[NSMutableArray alloc] init];
        _diagnostics = [[NSMutableArray alloc] init];
        _hits = [[NSMutableIndexSet alloc] init];
    }
    return self;
}

#if !__has_feature(objc_arc)
- (void)dealloc {
    [_relativePath release];
    [_archs release];
    [_diagnostics release];
    [_hits release];
    [super dealloc];
}
#endif

- (void)consume:(PMANameKind)kind bytes:(const char *)bytes length:(size_t)length {
    switch (kind) {
        case PMAKindImport:
            _importCount++;
            pma_match(kind, bytes, length, PMARecordHit, (__bridge void *)self);
            break;
        case PMAKindSelector:
            _selectorCount++;
            pma_match(kind, bytes, length, PMARecordHit, (__bridge void *)self);
            break;
        case PMAKindArch: {
            NSString *arch = PMA_AUTORELEASE([[NSString alloc] initWithBytes:bytes length:length
                                                                     encoding:NSUTF8StringEncoding]);
            if (arch && ![_archs containsObject:arch]) [_archs addObject:arch];
            break;
        }
        case PMAKindDiagnostic: {
            NSString *msg = PMA_AUTORELEASE([[NSString alloc] initWithBytes:bytes length:length
                                                                   encoding:NSUTF8StringEncoding]);
            if (!msg) msg = @"unreadable diagnostic";
            if (![_diagnostics containsObject:msg]) [_diagnostics addObject:msg];
            break;
        }
    }
}

- (NSString *)archSummary {
    return [_archs count] ? [_archs componentsJoinedByString:@","] : @"none";
}
@end

@interface PMAManifest : NSObject {
    NSString *_relativePath;
    NSMutableDictionary *_declared;
    BOOL _readable;
}
@property (nonatomic, copy) NSString *relativePath;
@property (nonatomic, strong) NSMutableDictionary *declared;
@property (nonatomic, assign) BOOL readable;
@end

@implementation PMAManifest
@synthesize relativePath = _relativePath;
@synthesize declared = _declared;
@synthesize readable = _readable;

- (instancetype)init {
    if ((self = [super init])) {
        _declared = [[NSMutableDictionary alloc] init];
    }
    return self;
}

#if !__has_feature(objc_arc)
- (void)dealloc {
    [_relativePath release];
    [_declared release];
    [super dealloc];
}
#endif
@end

@interface PMABundle : NSObject {
    NSString *_relativePath;
    NSString *_displayName;
    NSString *_kind;
    NSMutableArray *_binaries;
    NSMutableArray *_manifests;
}
@property (nonatomic, copy) NSString *relativePath;
@property (nonatomic, copy) NSString *displayName;
@property (nonatomic, copy) NSString *kind;
@property (nonatomic, strong) NSMutableArray *binaries;
@property (nonatomic, strong) NSMutableArray *manifests;
- (NSString *)frameworkName;
@end

@implementation PMABundle
@synthesize relativePath = _relativePath;
@synthesize displayName = _displayName;
@synthesize kind = _kind;
@synthesize binaries = _binaries;
@synthesize manifests = _manifests;

- (instancetype)init {
    if ((self = [super init])) {
        _binaries = [[NSMutableArray alloc] init];
        _manifests = [[NSMutableArray alloc] init];
    }
    return self;
}

#if !__has_feature(objc_arc)
- (void)dealloc {
    [_relativePath release];
    [_displayName release];
    [_kind release];
    [_binaries release];
    [_manifests release];
    [super dealloc];
}
#endif

- (NSString *)frameworkName {
    if (![_kind isEqualToString:@"framework"]) return nil;
    return [[_relativePath lastPathComponent] stringByDeletingPathExtension];
}
@end

/* ------------------------------------------------------------------------ */
/* Auditor                                                                  */
/* ------------------------------------------------------------------------ */

typedef struct {
    BOOL json;
    BOOL skeleton;
    BOOL unionScope;
    BOOL heuristics;
    BOOL strict;
    BOOL quiet;
} PMAOptions;

@interface PMAAuditor : NSObject {
    NSString *_inputPath;
    NSString *_baseDir;
    NSString *_rootName;
    NSString *_rootKind;
    NSArray *_extraManifests;
    PMAOptions _options;
    NSMutableDictionary *_bundles;
    NSMutableSet *_codeBundleDirs;
    NSMutableArray *_findings;
    NSMutableDictionary *_missingByBundle;
    NSUInteger _binaryCount;
    NSUInteger _manifestCount;
}
- (instancetype)initWithPath:(NSString *)path options:(PMAOptions)options extraManifests:(NSArray *)extra;
- (int)run;
@end

@implementation PMAAuditor

- (instancetype)initWithPath:(NSString *)path options:(PMAOptions)options extraManifests:(NSArray *)extra {
    if ((self = [super init])) {
        _inputPath = [[path stringByStandardizingPath] copy];
        _options = options;
        _extraManifests = [extra copy];
        _bundles = [[NSMutableDictionary alloc] init];
        _codeBundleDirs = [[NSMutableSet alloc] init];
        _findings = [[NSMutableArray alloc] init];
        _missingByBundle = [[NSMutableDictionary alloc] init];
    }
    return self;
}

#if !__has_feature(objc_arc)
- (void)dealloc {
    [_inputPath release];
    [_baseDir release];
    [_rootName release];
    [_rootKind release];
    [_extraManifests release];
    [_bundles release];
    [_codeBundleDirs release];
    [_findings release];
    [_missingByBundle release];
    [super dealloc];
}
#endif

- (void)addFinding:(NSString *)severity
              code:(NSString *)code
              path:(NSString *)path
           message:(NSString *)message
          evidence:(NSArray *)evidence
         reference:(NSString *)reference {
    NSMutableDictionary *f = [NSMutableDictionary dictionary];
    [f setObject:severity forKey:@"severity"];
    [f setObject:code forKey:@"code"];
    [f setObject:path forKey:@"path"];
    [f setObject:message forKey:@"message"];
    [f setObject:(evidence ? evidence : [NSArray array]) forKey:@"evidence"];
    if (reference) [f setObject:reference forKey:@"reference"];
    [_findings addObject:f];
}

- (NSString *)displayPathFor:(NSString *)relative {
    if ([relative isEqualToString:@"."]) return _rootName;
    if ([_rootKind isEqualToString:@"file"]) return relative;
    return [_rootName stringByAppendingPathComponent:relative];
}

- (PMABundle *)bundleAt:(NSString *)relative kind:(NSString *)kind {
    PMABundle *bundle = [_bundles objectForKey:relative];
    if (!bundle) {
        bundle = PMA_AUTORELEASE([[PMABundle alloc] init]);
        [bundle setRelativePath:relative];
        [bundle setKind:kind];
        [bundle setDisplayName:[self displayPathFor:relative]];
        [_bundles setObject:bundle forKey:relative];
    }
    return bundle;
}

static BOOL PMAIsCodeBundleExtension(NSString *ext) {
    static const char *exts[] = {"app", "appex", "framework", "xpc", "plugin", "kext", "systemextension", "bundle"};
    for (size_t i = 0; i < sizeof exts / sizeof exts[0]; i++) {
        if ([ext isEqualToString:PMAStr(exts[i])]) return YES;
    }
    return NO;
}

static BOOL PMAShouldSkip(NSArray *components) {
    for (NSString *c in components) {
        if ([[c pathExtension] isEqualToString:@"dSYM"] || [c isEqualToString:@"SwiftSupport"] ||
            [c isEqualToString:@"BCSymbolMaps"] || [c isEqualToString:@"__MACOSX"] ||
            [c isEqualToString:@"_CodeSignature"]) {
            return YES;
        }
    }
    return NO;
}

/* Nearest enclosing bundle. Resource bundles without code pass through to their parent. */
- (NSString *)ownerOf:(NSString *)relative forBinary:(BOOL)forBinary kind:(NSString **)kindOut {
    NSArray *parts = [relative pathComponents];
    for (NSInteger i = (NSInteger)[parts count] - 2; i >= 0; i--) {
        NSString *component = [parts objectAtIndex:(NSUInteger)i];
        NSString *ext = [component pathExtension];
        NSString *prefix = [NSString pathWithComponents:[parts subarrayWithRange:NSMakeRange(0, (NSUInteger)i + 1)]];
        NSString *parentExt = i > 0 ? [[parts objectAtIndex:(NSUInteger)i - 1] pathExtension] : _rootKind;
        BOOL sliceDir = [parentExt isEqualToString:@"xcframework"] && !PMAIsCodeBundleExtension(ext);
        if (sliceDir) {
            if (kindOut) *kindOut = @"xcframework-slice";
            return prefix;
        }
        if (PMAIsCodeBundleExtension(ext)) {
            if ([ext isEqualToString:@"bundle"] && !forBinary && ![_codeBundleDirs containsObject:prefix]) continue;
            if (kindOut) *kindOut = ext;
            return prefix;
        }
    }
    if (kindOut) *kindOut = _rootKind;
    return @".";
}

- (BOOL)prepare {
    NSFileManager *fm = [NSFileManager defaultManager];
    BOOL isDir = NO;
    if (![fm fileExistsAtPath:_inputPath isDirectory:&isDir]) {
        PMAPrintErr([NSString stringWithFormat:@"error: %@ does not exist\n", _inputPath]);
        return NO;
    }
    NSString *ext = [[_inputPath pathExtension] lowercaseString];
    if (!isDir && ([ext isEqualToString:@"ipa"] || [ext isEqualToString:@"zip"])) {
        PMAPrintErr([NSString stringWithFormat:@"error: %@ is a zip archive. Extract it first, for example: "
                                                @"unzip -q %@ -d extracted, then scan extracted/Payload\n",
                                               _inputPath, [_inputPath lastPathComponent]]);
        return NO;
    }
    _rootName = [[_inputPath lastPathComponent] copy];
    if (isDir) {
        _baseDir = [_inputPath copy];
        _rootKind = [([ext length] ? ext : @"directory") copy];
    } else {
        _baseDir = [[_inputPath stringByDeletingLastPathComponent] copy];
        _rootKind = [@"file" copy];
    }
    return YES;
}

- (NSArray *)candidateFiles {
    NSFileManager *fm = [NSFileManager defaultManager];
    NSMutableArray *files = [NSMutableArray array];
    if ([_rootKind isEqualToString:@"file"]) {
        [files addObject:_rootName];
        return files;
    }
    NSMutableSet *seenInodes = [NSMutableSet set];
    NSDirectoryEnumerator *en = [fm enumeratorAtPath:_baseDir];
    NSString *rel;
    while ((rel = [en nextObject])) {
        NSDictionary *attrs = [en fileAttributes];
        if (![[attrs fileType] isEqualToString:NSFileTypeRegular]) continue;
        if (PMAShouldSkip([rel pathComponents])) continue;
        NSString *inode = [NSString stringWithFormat:@"%lu:%lu", (unsigned long)[attrs fileSystemNumber],
                                                    (unsigned long)[attrs fileSystemFileNumber]];
        if ([seenInodes containsObject:inode]) continue;
        [seenInodes addObject:inode];
        [files addObject:rel];
    }
    return [files sortedArrayUsingFunction:PMACompareStrings context:NULL];
}

- (PMABinaryScan *)scanBinaryAt:(NSString *)absolute relative:(NSString *)relative sniffOnly:(BOOL *)notBinary {
    *notBinary = YES;
    int fd = open([absolute fileSystemRepresentation], O_RDONLY);
    if (fd < 0) return nil;
    struct stat sb;
    if (fstat(fd, &sb) != 0 || sb.st_size < 8) {
        close(fd);
        return nil;
    }
    uint8_t head[8];
    if (pread(fd, head, sizeof head, 0) != (ssize_t)sizeof head || !pma_sniff_magic(head, sizeof head)) {
        close(fd);
        return nil;
    }
    *notBinary = NO;
    PMABinaryScan *scan = PMA_AUTORELEASE([[PMABinaryScan alloc] init]);
    [scan setRelativePath:relative];
    size_t size = (size_t)sb.st_size;
    void *map = mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (map == MAP_FAILED) {
        [[scan diagnostics] addObject:[NSString stringWithFormat:@"could not map file: %s", strerror(errno)]];
        return scan;
    }
    PMAScanState st;
    st.sink = PMACollect;
    st.ctx = (__bridge void *)scan;
    st.depth = 0;
    st.member = NULL;
    pma_scan_buffer(&st, (const uint8_t *)map, size);
    munmap(map, size);
    return scan;
}

- (void)loadManifest:(NSString *)absolute relative:(NSString *)relative into:(PMABundle *)bundle {
    PMAManifest *manifest = PMA_AUTORELEASE([[PMAManifest alloc] init]);
    [manifest setRelativePath:relative];
    [[bundle manifests] addObject:manifest];
    _manifestCount++;
    NSString *where = [self displayPathFor:relative];
    if ([relative hasPrefix:@"/"]) where = relative;

    NSData *data = [NSData dataWithContentsOfFile:absolute];
    if (!data) {
        [self addFinding:@"error" code:@"PMA201" path:where message:@"Privacy manifest could not be read"
                evidence:nil reference:nil];
        return;
    }
    NSError *error = nil;
    id plist = [NSPropertyListSerialization propertyListWithData:data options:NSPropertyListImmutable
                                                          format:NULL error:&error];
    if (![plist isKindOfClass:[NSDictionary class]]) {
        NSString *why = error ? [error localizedDescription] : @"root object is not a dictionary";
        [self addFinding:@"error" code:@"PMA201" path:where
                 message:[NSString stringWithFormat:@"Privacy manifest is not a valid property list: %@", why]
                evidence:nil reference:nil];
        return;
    }
    [manifest setReadable:YES];
    NSDictionary *root = plist;

    id apiTypes = [root objectForKey:@"NSPrivacyAccessedAPITypes"];
    if (apiTypes && ![apiTypes isKindOfClass:[NSArray class]]) {
        [self addFinding:@"error" code:@"PMA202" path:where
                 message:@"NSPrivacyAccessedAPITypes must be an array of dictionaries" evidence:nil reference:nil];
        apiTypes = nil;
    }
    NSUInteger index = 0;
    for (id entry in (NSArray *)apiTypes) {
        index++;
        if (![entry isKindOfClass:[NSDictionary class]]) {
            [self addFinding:@"error" code:@"PMA202" path:where
                     message:[NSString stringWithFormat:@"NSPrivacyAccessedAPITypes item %lu is not a dictionary",
                                                        (unsigned long)index]
                    evidence:nil reference:nil];
            continue;
        }
        id category = [entry objectForKey:@"NSPrivacyAccessedAPIType"];
        id reasons = [entry objectForKey:@"NSPrivacyAccessedAPITypeReasons"];
        if (![category isKindOfClass:[NSString class]]) {
            [self addFinding:@"error" code:@"PMA202" path:where
                     message:[NSString stringWithFormat:@"NSPrivacyAccessedAPITypes item %lu has no "
                                                        @"NSPrivacyAccessedAPIType string",
                                                        (unsigned long)index]
                    evidence:nil reference:nil];
            continue;
        }
        if (!PMAIsKnownCategory(category)) {
            NSString *hint = @"";
            for (size_t i = 0; i < PMA_CATEGORY_COUNT; i++) {
                NSString *known = PMAStr(kPMACategories[i]);
                NSString *trimmed = [category stringByTrimmingCharactersInSet:[NSCharacterSet whitespaceAndNewlineCharacterSet]];
                if ([[known lowercaseString] isEqualToString:[trimmed lowercaseString]]) {
                    hint = [NSString stringWithFormat:@". Did you mean %@? The match is exact and case sensitive", known];
                }
            }
            [self addFinding:@"error" code:@"PMA203" path:where
                     message:[NSString stringWithFormat:@"Unknown API category \"%@\"%@", category, hint]
                    evidence:nil reference:nil];
            continue;
        }
        if ([[manifest declared] objectForKey:category]) {
            [self addFinding:@"warning" code:@"PMA207" path:where
                     message:[NSString stringWithFormat:@"%@ is declared more than once; entries were merged",
                                                        PMAShortCategory(category)]
                    evidence:nil reference:nil];
        }
        NSMutableArray *codes = [[manifest declared] objectForKey:category];
        if (!codes) {
            codes = [NSMutableArray array];
            [[manifest declared] setObject:codes forKey:category];
        }
        if (![reasons isKindOfClass:[NSArray class]] || [reasons count] == 0) {
            [self addFinding:@"error" code:@"PMA206" path:where
                     message:[NSString stringWithFormat:@"%@ is declared with no reasons. Apple rejects an empty "
                                                        @"NSPrivacyAccessedAPITypeReasons array",
                                                        PMAShortCategory(category)]
                    evidence:nil reference:@"ITMS-91056"];
            continue;
        }
        for (id code in (NSArray *)reasons) {
            if (![code isKindOfClass:[NSString class]]) {
                [self addFinding:@"error" code:@"PMA202" path:where
                         message:[NSString stringWithFormat:@"A reason under %@ is not a string",
                                                            PMAShortCategory(category)]
                        evidence:nil reference:nil];
                continue;
            }
            NSString *owner = PMACategoryForReason(code);
            if (owner && [owner isEqualToString:category]) {
                if (![codes containsObject:code]) [codes addObject:code];
                continue;
            }
            if (owner) {
                [self addFinding:@"error" code:@"PMA205" path:where
                         message:[NSString stringWithFormat:@"Reason %@ belongs to %@, not %@", code,
                                                            PMAShortCategory(owner), PMAShortCategory(category)]
                        evidence:nil reference:@"ITMS-91056"];
                continue;
            }
            NSString *trimmed = [[code stringByTrimmingCharactersInSet:[NSCharacterSet whitespaceAndNewlineCharacterSet]]
                uppercaseString];
            NSString *near = PMACategoryForReason(trimmed);
            NSString *hint = near ? [NSString stringWithFormat:@". Write it exactly as %@", trimmed] : @"";
            [self addFinding:@"error" code:@"PMA204" path:where
                     message:[NSString stringWithFormat:@"Reason \"%@\" is not an approved code for %@%@", code,
                                                        PMAShortCategory(category), hint]
                    evidence:nil reference:@"ITMS-91056"];
        }
    }

    id tracking = [root objectForKey:@"NSPrivacyTracking"];
    id domains = [root objectForKey:@"NSPrivacyTrackingDomains"];
    if (tracking && ![tracking isKindOfClass:[NSNumber class]]) {
        [self addFinding:@"error" code:@"PMA202" path:where message:@"NSPrivacyTracking must be a boolean"
                evidence:nil reference:nil];
        tracking = nil;
    }
    if (domains && ![domains isKindOfClass:[NSArray class]]) {
        [self addFinding:@"error" code:@"PMA202" path:where
                 message:@"NSPrivacyTrackingDomains must be an array of strings" evidence:nil reference:nil];
        domains = nil;
    }
    BOOL tracks = tracking ? [tracking boolValue] : NO;
    NSUInteger domainCount = domains ? [domains count] : 0;
    if (tracks && domainCount == 0) {
        [self addFinding:@"warning" code:@"PMA208" path:where
                 message:@"NSPrivacyTracking is true but NSPrivacyTrackingDomains is empty. If the code contacts "
                         @"tracking hosts, list them so iOS can block them without ATT consent"
                evidence:nil reference:nil];
    }
    if (!tracks && domainCount > 0) {
        [self addFinding:@"warning" code:@"PMA209" path:where
                 message:[NSString stringWithFormat:@"%lu tracking domains are listed while NSPrivacyTracking is "
                                                    @"false or missing. Either the code tracks or the list is stale",
                                                    (unsigned long)domainCount]
                evidence:PMASorted(domains) reference:nil];
    }

    id collected = [root objectForKey:@"NSPrivacyCollectedDataTypes"];
    if (collected && ![collected isKindOfClass:[NSArray class]]) {
        [self addFinding:@"error" code:@"PMA202" path:where
                 message:@"NSPrivacyCollectedDataTypes must be an array of dictionaries" evidence:nil reference:nil];
        collected = nil;
    }
    index = 0;
    for (id item in (NSArray *)collected) {
        index++;
        if (![item isKindOfClass:[NSDictionary class]]) {
            [self addFinding:@"error" code:@"PMA202" path:where
                     message:[NSString stringWithFormat:@"NSPrivacyCollectedDataTypes item %lu is not a dictionary",
                                                        (unsigned long)index]
                    evidence:nil reference:nil];
            continue;
        }
        id type = [item objectForKey:@"NSPrivacyCollectedDataType"];
        NSString *prefix = @"NSPrivacyCollectedDataType";
        if (![type isKindOfClass:[NSString class]]) {
            [self addFinding:@"error" code:@"PMA202" path:where
                     message:[NSString stringWithFormat:@"NSPrivacyCollectedDataTypes item %lu has no "
                                                        @"NSPrivacyCollectedDataType string",
                                                        (unsigned long)index]
                    evidence:nil reference:nil];
        } else if (![type hasPrefix:prefix] ||
                   !PMAListContains(kPMADataTypes, PMA_DATA_TYPE_COUNT, [type substringFromIndex:[prefix length]])) {
            [self addFinding:@"warning" code:@"PMA210" path:where
                     message:[NSString stringWithFormat:@"Unrecognised collected data type \"%@\"", type]
                    evidence:nil reference:nil];
        }
        if (![[item objectForKey:@"NSPrivacyCollectedDataTypeLinked"] isKindOfClass:[NSNumber class]] ||
            ![[item objectForKey:@"NSPrivacyCollectedDataTypeTracking"] isKindOfClass:[NSNumber class]]) {
            [self addFinding:@"error" code:@"PMA202" path:where
                     message:[NSString stringWithFormat:@"NSPrivacyCollectedDataTypes item %lu needs boolean "
                                                        @"NSPrivacyCollectedDataTypeLinked and "
                                                        @"NSPrivacyCollectedDataTypeTracking",
                                                        (unsigned long)index]
                    evidence:nil reference:nil];
        }
        id purposes = [item objectForKey:@"NSPrivacyCollectedDataTypePurposes"];
        if (![purposes isKindOfClass:[NSArray class]] || [purposes count] == 0) {
            [self addFinding:@"error" code:@"PMA202" path:where
                     message:[NSString stringWithFormat:@"NSPrivacyCollectedDataTypes item %lu has no purposes",
                                                        (unsigned long)index]
                    evidence:nil reference:nil];
            continue;
        }
        NSString *purposePrefix = @"NSPrivacyCollectedDataTypePurpose";
        for (id purpose in (NSArray *)purposes) {
            if (![purpose isKindOfClass:[NSString class]] || ![purpose hasPrefix:purposePrefix] ||
                !PMAListContains(kPMAPurposes, PMA_PURPOSE_COUNT, [purpose substringFromIndex:[purposePrefix length]])) {
                [self addFinding:@"warning" code:@"PMA211" path:where
                         message:[NSString stringWithFormat:@"Unrecognised collection purpose \"%@\"", purpose]
                        evidence:nil reference:nil];
            }
        }
    }
}

- (BOOL)discover {
    NSArray *files = [self candidateFiles];
    NSMutableArray *manifestPaths = [NSMutableArray array];

    for (NSString *rel in files) {
        if ([[rel lastPathComponent] isEqualToString:@"PrivacyInfo.xcprivacy"]) {
            [manifestPaths addObject:rel];
            continue;
        }
        NSString *absolute = [_baseDir stringByAppendingPathComponent:rel];
        @autoreleasepool {
            BOOL notBinary = YES;
            PMABinaryScan *scan = [self scanBinaryAt:absolute relative:rel sniffOnly:&notBinary];
            if (!notBinary && scan && ([[scan archs] count] || [[scan diagnostics] count])) {
                NSString *kind = nil;
                NSString *owner = [self ownerOf:rel forBinary:YES kind:&kind];
                if ([[owner pathExtension] isEqualToString:@"bundle"]) [_codeBundleDirs addObject:owner];
                [[[self bundleAt:owner kind:kind] binaries] addObject:scan];
                _binaryCount++;
            }
        }
    }

    for (NSString *rel in manifestPaths) {
        NSString *kind = nil;
        NSString *owner = [self ownerOf:rel forBinary:NO kind:&kind];
        [self loadManifest:[_baseDir stringByAppendingPathComponent:rel] relative:rel
                      into:[self bundleAt:owner kind:kind]];
    }
    for (NSString *extra in _extraManifests) {
        NSString *absolute = [extra stringByStandardizingPath];
        [self loadManifest:absolute relative:absolute into:[self bundleAt:@"." kind:_rootKind]];
    }
    return YES;
}

- (NSDictionary *)declaredFor:(NSArray *)manifests {
    NSMutableDictionary *merged = [NSMutableDictionary dictionary];
    for (PMAManifest *m in manifests) {
        for (NSString *category in [m declared]) {
            if ([[[m declared] objectForKey:category] count] == 0) continue;
            NSMutableSet *codes = [merged objectForKey:category];
            if (!codes) {
                codes = [NSMutableSet set];
                [merged setObject:codes forKey:category];
            }
            [codes addObjectsFromArray:[[m declared] objectForKey:category]];
        }
    }
    return merged;
}

static NSArray *PMACapEvidence(NSArray *items) {
    NSArray *sorted = PMASorted(items);
    if ([sorted count] <= PMA_EVIDENCE_LIMIT) return sorted;
    NSMutableArray *capped = [NSMutableArray arrayWithArray:[sorted subarrayWithRange:NSMakeRange(0, PMA_EVIDENCE_LIMIT)]];
    [capped addObject:[NSString stringWithFormat:@"and %lu more", (unsigned long)([sorted count] - PMA_EVIDENCE_LIMIT)]];
    return capped;
}

- (void)evaluate {
    NSMutableArray *allManifests = [NSMutableArray array];
    for (PMABundle *bundle in [_bundles allValues]) [allManifests addObjectsFromArray:[bundle manifests]];
    NSDictionary *globalDeclared = [self declaredFor:allManifests];

    for (NSString *key in PMASorted([_bundles allKeys])) {
        PMABundle *bundle = [_bundles objectForKey:key];
        NSString *where = [bundle displayName];
        NSDictionary *ownDeclared = [self declaredFor:[bundle manifests]];
        NSDictionary *declared = _options.unionScope ? globalDeclared : ownDeclared;

        NSMutableDictionary *direct = [NSMutableDictionary dictionary];
        NSMutableDictionary *heuristic = [NSMutableDictionary dictionary];
        NSMutableArray *ambiguous = [NSMutableArray array];

        for (PMABinaryScan *scan in [bundle binaries]) {
            NSString *binName = [self displayPathFor:[scan relativePath]];
            for (NSString *diag in [scan diagnostics]) {
                [self addFinding:@"warning" code:@"PMA401" path:binName
                         message:[NSString stringWithFormat:@"Binary was only partly readable: %@", diag]
                        evidence:nil reference:nil];
            }
            if ([[scan archs] count] && [scan importCount] == 0) {
                [self addFinding:@"note" code:@"PMA402" path:binName
                         message:@"No imported symbols were found. Results for this binary rely on selectors only"
                        evidence:nil reference:nil];
            }
            NSUInteger r = [[scan hits] firstIndex];
            while (r != NSNotFound) {
                const PMARule *rule = &kPMARules[r];
                NSString *line = [NSString stringWithFormat:@"%@ [%@]: %s", binName, [scan archSummary], rule->label];
                if (rule->alternate) {
                    [ambiguous addObject:line];
                } else if (!rule->heuristic || _options.heuristics) {
                    NSMutableDictionary *target = rule->heuristic ? heuristic : direct;
                    NSString *category = PMAStr(rule->category);
                    NSMutableArray *list = [target objectForKey:category];
                    if (!list) {
                        list = [NSMutableArray array];
                        [target setObject:list forKey:category];
                    }
                    if (![list containsObject:line]) [list addObject:line];
                }
                r = [[scan hits] indexGreaterThanIndex:r];
            }
        }

        NSString *scopeNote = _options.unionScope ? @"no privacy manifest anywhere in the scan"
                                                  : ([[bundle manifests] count]
                                                         ? @"no privacy manifest in this bundle"
                                                         : @"this bundle ships no PrivacyInfo.xcprivacy, so nothing");
        NSMutableArray *missing = [NSMutableArray array];

        for (NSString *category in PMASorted([direct allKeys])) {
            if ([declared objectForKey:category]) continue;
            [missing addObject:category];
            [self addFinding:@"error" code:@"PMA101" path:where
                     message:[NSString stringWithFormat:@"Uses %@ APIs but %@ declares %@", PMAShortCategory(category),
                                                        scopeNote, category]
                    evidence:PMACapEvidence([direct objectForKey:category])
                   reference:@"ITMS-91053"];
        }
        for (NSString *category in PMASorted([heuristic allKeys])) {
            if ([declared objectForKey:category] || [direct objectForKey:category]) continue;
            [missing addObject:category];
            [self addFinding:@"warning" code:@"PMA102" path:where
                     message:[NSString stringWithFormat:@"Probably uses %@ APIs through an indirect path and %@ "
                                                        @"declares %@",
                                                        PMAShortCategory(category), scopeNote, category]
                    evidence:PMACapEvidence([heuristic objectForKey:category])
                   reference:@"ITMS-91053"];
        }
        if ([ambiguous count] && ![declared objectForKey:@CAT_FILE] && ![declared objectForKey:@CAT_DISK]) {
            if (![missing containsObject:@CAT_FILE]) [missing addObject:@CAT_FILE];
            [self addFinding:@"error" code:@"PMA103" path:where
                     message:@"Calls the getattrlist family, which Apple files under both FileTimestamp and "
                             @"DiskSpace, and neither category is declared. Declare the one matching what the "
                             @"attribute list actually reads"
                    evidence:PMACapEvidence(ambiguous)
                   reference:@"ITMS-91053"];
        }
        if ([missing count]) [_missingByBundle setObject:missing forKey:key];

        if ([ownDeclared count]) {
            for (NSString *category in PMASorted([ownDeclared allKeys])) {
                BOOL used = [direct objectForKey:category] || [heuristic objectForKey:category] ||
                            ([ambiguous count] && ([category isEqualToString:@CAT_FILE] ||
                                                   [category isEqualToString:@CAT_DISK]));
                if (used || [[bundle binaries] count] == 0) continue;
                [self addFinding:@"note" code:@"PMA212" path:where
                         message:[NSString stringWithFormat:@"%@ is declared but no matching API was found in this "
                                                            @"bundle's binaries. Fine if the call is made through "
                                                            @"dlsym, a script runtime or code scanned elsewhere",
                                                            PMAShortCategory(category)]
                        evidence:nil reference:nil];
            }
        }

        NSString *framework = [bundle frameworkName];
        if (framework && [[bundle manifests] count] == 0 &&
            PMAListContains(kPMACommonSDKs, PMA_SDK_COUNT, framework)) {
            [self addFinding:@"error" code:@"PMA301" path:where
                     message:[NSString stringWithFormat:@"%@ is on Apple's list of commonly used SDKs and is embedded "
                                                        @"without its own privacy manifest. Upgrade to a release that "
                                                        @"ships PrivacyInfo.xcprivacy",
                                                        framework]
                    evidence:nil reference:@"ITMS-91061"];
        }
    }
}

- (NSUInteger)countSeverity:(NSString *)severity {
    NSUInteger n = 0;
    for (NSDictionary *f in _findings) {
        if ([[f objectForKey:@"severity"] isEqualToString:severity]) n++;
    }
    return n;
}

- (NSString *)skeletonText {
    NSMutableString *out = [NSMutableString string];
    for (NSString *key in PMASorted([_missingByBundle allKeys])) {
        PMABundle *bundle = [_bundles objectForKey:key];
        [out appendFormat:@"<!-- %@: merge into NSPrivacyAccessedAPITypes of its PrivacyInfo.xcprivacy -->\n",
                          [bundle displayName]];
        [out appendString:@"<key>NSPrivacyAccessedAPITypes</key>\n<array>\n"];
        for (NSString *category in PMASorted([_missingByBundle objectForKey:key])) {
            [out appendString:@"  <dict>\n    <key>NSPrivacyAccessedAPIType</key>\n"];
            [out appendFormat:@"    <string>%@</string>\n", category];
            [out appendString:@"    <key>NSPrivacyAccessedAPITypeReasons</key>\n    <array>\n"];
            [out appendString:@"      <!-- Keep only the codes that truly describe your use:\n"];
            for (size_t i = 0; i < PMA_REASON_COUNT; i++) {
                if (![category isEqualToString:PMAStr(kPMAReasons[i].category)]) continue;
                [out appendFormat:@"           <string>%s</string>  %s\n", kPMAReasons[i].code, kPMAReasons[i].summary];
            }
            [out appendString:@"      -->\n    </array>\n  </dict>\n"];
        }
        [out appendString:@"</array>\n\n"];
    }
    if ([out length] == 0) [out appendString:@"<!-- Nothing missing: every bundle declares the categories it uses -->\n"];
    return out;
}

- (NSDictionary *)reportObject:(NSArray *)findings {
    NSMutableArray *bundles = [NSMutableArray array];
    for (NSString *key in PMASorted([_bundles allKeys])) {
        PMABundle *bundle = [_bundles objectForKey:key];
        NSMutableArray *binaries = [NSMutableArray array];
        for (PMABinaryScan *scan in [bundle binaries]) {
            NSMutableArray *apis = [NSMutableArray array];
            NSUInteger r = [[scan hits] firstIndex];
            while (r != NSNotFound) {
                const PMARule *rule = &kPMARules[r];
                NSMutableDictionary *api = [NSMutableDictionary dictionary];
                [api setObject:PMAStr(rule->label) forKey:@"api"];
                [api setObject:PMAStr(rule->category) forKey:@"category"];
                if (rule->alternate) [api setObject:PMAStr(rule->alternate) forKey:@"alternateCategory"];
                [api setObject:(rule->heuristic ? @"heuristic" : @"direct") forKey:@"confidence"];
                [apis addObject:api];
                r = [[scan hits] indexGreaterThanIndex:r];
            }
            NSMutableDictionary *b = [NSMutableDictionary dictionary];
            [b setObject:[self displayPathFor:[scan relativePath]] forKey:@"path"];
            [b setObject:[scan archs] forKey:@"archs"];
            [b setObject:[NSNumber numberWithUnsignedLongLong:[scan importCount]] forKey:@"importCount"];
            [b setObject:[NSNumber numberWithUnsignedLongLong:[scan selectorCount]] forKey:@"selectorCount"];
            [b setObject:apis forKey:@"requiredReasonAPIs"];
            [b setObject:[scan diagnostics] forKey:@"diagnostics"];
            [binaries addObject:b];
        }
        NSMutableArray *manifests = [NSMutableArray array];
        for (PMAManifest *m in [bundle manifests]) {
            NSMutableDictionary *declared = [NSMutableDictionary dictionary];
            for (NSString *category in [m declared]) {
                [declared setObject:PMASorted([[m declared] objectForKey:category]) forKey:category];
            }
            NSMutableDictionary *md = [NSMutableDictionary dictionary];
            NSString *mp = [[m relativePath] hasPrefix:@"/"] ? [m relativePath] : [self displayPathFor:[m relativePath]];
            [md setObject:mp forKey:@"path"];
            [md setObject:declared forKey:@"declared"];
            [md setObject:([m readable] ? @"ok" : @"unreadable") forKey:@"status"];
            [manifests addObject:md];
        }
        NSMutableDictionary *bd = [NSMutableDictionary dictionary];
        [bd setObject:[bundle displayName] forKey:@"path"];
        [bd setObject:[bundle kind] forKey:@"kind"];
        [bd setObject:binaries forKey:@"binaries"];
        [bd setObject:manifests forKey:@"manifests"];
        [bundles addObject:bd];
    }
    NSMutableDictionary *summary = [NSMutableDictionary dictionary];
    [summary setObject:[NSNumber numberWithUnsignedInteger:[self countSeverity:@"error"]] forKey:@"errors"];
    [summary setObject:[NSNumber numberWithUnsignedInteger:[self countSeverity:@"warning"]] forKey:@"warnings"];
    [summary setObject:[NSNumber numberWithUnsignedInteger:[self countSeverity:@"note"]] forKey:@"notes"];
    [summary setObject:[NSNumber numberWithUnsignedInteger:[_bundles count]] forKey:@"bundles"];
    [summary setObject:[NSNumber numberWithUnsignedInteger:_binaryCount] forKey:@"binaries"];
    [summary setObject:[NSNumber numberWithUnsignedInteger:_manifestCount] forKey:@"manifests"];

    NSMutableDictionary *report = [NSMutableDictionary dictionary];
    [report setObject:@"PrivacyManifestAuditor" forKey:@"tool"];
    [report setObject:@PMA_VERSION forKey:@"version"];
    [report setObject:_rootName forKey:@"input"];
    [report setObject:(_options.unionScope ? @"union" : @"bundle") forKey:@"scope"];
    [report setObject:summary forKey:@"summary"];
    [report setObject:bundles forKey:@"bundles"];
    [report setObject:findings forKey:@"findings"];
    return report;
}

- (NSString *)textReport:(NSArray *)findings {
    NSMutableString *out = [NSMutableString string];
    [out appendFormat:@"Privacy Manifest Auditor %s\n", PMA_VERSION];
    [out appendFormat:@"Scanned %@: %lu bundles, %lu binaries, %lu manifests (scope: %@)\n\n", _rootName,
                      (unsigned long)[_bundles count], (unsigned long)_binaryCount, (unsigned long)_manifestCount,
                      _options.unionScope ? @"union" : @"bundle"];
    NSUInteger shown = 0;
    for (NSDictionary *f in findings) {
        NSString *severity = [f objectForKey:@"severity"];
        if (_options.quiet && [severity isEqualToString:@"note"]) continue;
        shown++;
        NSString *ref = [f objectForKey:@"reference"];
        [out appendFormat:@"%-7s %@  %@\n", [severity UTF8String], [f objectForKey:@"code"], [f objectForKey:@"path"]];
        [out appendFormat:@"        %@%@\n", [f objectForKey:@"message"],
                          ref ? [NSString stringWithFormat:@" (%@)", ref] : @""];
        for (NSString *e in [f objectForKey:@"evidence"]) [out appendFormat:@"        - %@\n", e];
        [out appendString:@"\n"];
    }
    if (shown == 0) [out appendString:@"No findings.\n\n"];
    NSUInteger errors = [self countSeverity:@"error"];
    NSUInteger warnings = [self countSeverity:@"warning"];
    NSUInteger notes = [self countSeverity:@"note"];
    [out appendFormat:@"Summary: %lu error%s, %lu warning%s, %lu note%s\n", (unsigned long)errors, errors == 1 ? "" : "s",
                      (unsigned long)warnings, warnings == 1 ? "" : "s", (unsigned long)notes, notes == 1 ? "" : "s"];
    return out;
}

- (int)run {
    if (![self prepare]) return PMA_EXIT_USAGE;
    [self discover];
    if (_binaryCount == 0 && _manifestCount == 0) {
        PMAPrintErr([NSString stringWithFormat:@"error: no Mach-O binaries or privacy manifests found under %@\n",
                                               _inputPath]);
        return PMA_EXIT_USAGE;
    }
    [self evaluate];
    NSArray *findings = [_findings sortedArrayUsingFunction:PMACompareFindings context:NULL];

    if (_options.json) {
        NSMutableString *out = [NSMutableString string];
        PMAJSONAppend(out, [self reportObject:findings], 0);
        [out appendString:@"\n"];
        PMAPrint(out);
    } else if (_options.skeleton) {
        PMAPrint([self skeletonText]);
    } else {
        PMAPrint([self textReport:findings]);
    }

    NSUInteger errors = [self countSeverity:@"error"];
    NSUInteger warnings = [self countSeverity:@"warning"];
    if (errors > 0 || (_options.strict && warnings > 0)) return PMA_EXIT_FINDINGS;
    return PMA_EXIT_CLEAN;
}
@end

/* ------------------------------------------------------------------------ */
/* Entry point                                                              */
/* ------------------------------------------------------------------------ */

static void PMAUsage(FILE *to) {
    fprintf(to,
            "usage: PrivacyManifestAuditor [options] <path>\n"
            "\n"
            "<path> is an .app, .xcarchive, .xcframework, .framework, an extracted IPA Payload folder,\n"
            "a single Mach-O file or a static library (.a).\n"
            "\n"
            "  --json              machine readable report with sorted keys\n"
            "  --skeleton          print PrivacyInfo.xcprivacy snippets for every missing category\n"
            "  --union             let any manifest in the scan cover every binary (lenient)\n"
            "  --no-heuristics     only report APIs from Apple's published list\n"
            "  --strict            exit 1 on warnings as well as errors\n"
            "  --quiet             hide notes in the text report\n"
            "  --manifest <file>   extra PrivacyInfo.xcprivacy attached to the root bundle (repeatable)\n"
            "  --version           print the version\n"
            "  -h, --help          show this help\n"
            "\n"
            "exit status: 0 clean, 1 findings, 2 usage or input error\n");
}

static int PMAMain(int argc, const char *argv[]) {
    PMAOptions options;
    options.json = NO;
    options.skeleton = NO;
    options.unionScope = NO;
    options.heuristics = YES;
    options.strict = NO;
    options.quiet = NO;
    NSMutableArray *extra = [NSMutableArray array];
    NSString *path = nil;

    for (int i = 1; i < argc; i++) {
        const char *a = argv[i];
        if (strcmp(a, "--json") == 0) {
            options.json = YES;
        } else if (strcmp(a, "--skeleton") == 0) {
            options.skeleton = YES;
        } else if (strcmp(a, "--union") == 0) {
            options.unionScope = YES;
        } else if (strcmp(a, "--no-heuristics") == 0) {
            options.heuristics = NO;
        } else if (strcmp(a, "--strict") == 0) {
            options.strict = YES;
        } else if (strcmp(a, "--quiet") == 0) {
            options.quiet = YES;
        } else if (strcmp(a, "--manifest") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "error: --manifest needs a file path\n");
                return PMA_EXIT_USAGE;
            }
            [extra addObject:[NSString stringWithUTF8String:argv[++i]]];
        } else if (strcmp(a, "--version") == 0) {
            printf("PrivacyManifestAuditor %s\n", PMA_VERSION);
            return PMA_EXIT_CLEAN;
        } else if (strcmp(a, "-h") == 0 || strcmp(a, "--help") == 0) {
            PMAUsage(stdout);
            return PMA_EXIT_CLEAN;
        } else if (a[0] == '-' && a[1] != 0) {
            fprintf(stderr, "error: unknown option %s\n\n", a);
            PMAUsage(stderr);
            return PMA_EXIT_USAGE;
        } else if (path) {
            fprintf(stderr, "error: pass exactly one path\n");
            return PMA_EXIT_USAGE;
        } else {
            path = [NSString stringWithUTF8String:a];
        }
    }
    if (!path) {
        PMAUsage(stderr);
        return PMA_EXIT_USAGE;
    }
    if (options.json && options.skeleton) {
        fprintf(stderr, "error: choose either --json or --skeleton\n");
        return PMA_EXIT_USAGE;
    }
    PMAAuditor *auditor = PMA_AUTORELEASE([[PMAAuditor alloc] initWithPath:path options:options extraManifests:extra]);
    return [auditor run];
}

int main(int argc, const char *argv[]) {
    int status;
    @autoreleasepool {
        status = PMAMain(argc, argv);
    }
    return status;
}
