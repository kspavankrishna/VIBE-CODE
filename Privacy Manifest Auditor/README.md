# Privacy Manifest Auditor

An Objective-C command line tool that reads the compiled binaries inside an iOS or macOS app, finds every required reason API they call and checks that a PrivacyInfo.xcprivacy file in the right bundle declares it with a valid reason code. It catches ITMS-91053, ITMS-91056 and ITMS-91061 rejections on your machine or in CI, before App Store Connect emails you about them.

**Language:** Objective-C | **Lines:** 1885 | **Added:** 2026-11-16

## What this solves

Since May 2024 Apple rejects uploads whose code touches a "required reason API" without a matching declaration in a privacy manifest. The list covers file timestamps, system boot time, disk space, active keyboards and user defaults. It sounds small until you notice what is on it: `stat()`, `mach_absolute_time()`, `NSUserDefaults`. Almost every app and almost every third party SDK uses at least one of them.

The rejection email names a category and sometimes a file. It does not tell you which symbol triggered it, which framework owns that symbol or whether the manifest you already added is being read at all. Teams end up uploading build after build to find out. Three failure modes keep coming back:

1. **The API is in a framework, the declaration is in the app.** Each embedded framework needs its own manifest for its own binary. Declaring the category once in the app target does not cover a dynamic framework from a vendor.
2. **The manifest exists but says the wrong thing.** A reason code under the wrong category, a code with a stray space, a category name with the wrong case, an empty reasons array. All of these look fine in Xcode and all of them fail review.
3. **An SDK from Apple's commonly used list ships without a manifest.** Old releases of Alamofire, Firebase, SDWebImage, Flutter plugins and dozens more get the upload rejected under ITMS-91061 no matter what the app declares.

Privacy Manifest Auditor answers all three from the real build output. It does not grep your source tree, because source scans miss precompiled SDKs, static libraries, Swift overlays and anything pulled in by CocoaPods, Carthage or Swift Package Manager. It parses the Mach-O files that will actually ship.

## Why I built it

I kept seeing the same loop on app teams: archive, upload, wait for the email, guess which pod caused it, bump something, upload again. Each round costs a build slot and often a day. Apple's own privacy report in Xcode aggregates the manifests you have, but it does not tell you what your binaries need. Most scripts I found online search source files for the string `UserDefaults`, which is both too loud and too quiet: it flags comments and misses a vendor binary that calls `statfs()`.

I wanted one tool that looks at symbols the way the App Store pipeline does, explains each finding with the exact binary, architecture and API. It also exits non zero so CI can block the merge. It had to work on an app, an archive, an xcframework a vendor sent you or a single static library you are about to publish.

## When to use it

- In CI after `xcodebuild archive`, pointed at the `.xcarchive`, so a pull request that adds an SDK cannot merge if it breaks App Store upload.
- Before you upgrade or add a pod or package, pointed at the vendor's `.xcframework`, to see what you are taking on.
- If you publish an SDK, pointed at your own `.xcframework` or `.a`, so your customers never see ITMS-91053 because of you.
- After a rejection email, pointed at the extracted IPA, to find the exact binary and symbol behind it.
- On macOS apps headed for the Mac App Store, where the same rules apply and frameworks use the versioned `Versions/A` layout.

Skip it if you only need the privacy nutrition label for App Store Connect. That label is about collected data. This tool only sanity checks the collected data section of the manifest.

## How it works

The file has two layers. The bottom layer is plain C with no Foundation calls, so it can be fuzzed on its own. The top layer is Objective-C and handles the file system, property lists, the rules and the report.

**Reading binaries.** `pma_scan_buffer` sniffs the first bytes and dispatches to one of three readers. `pma_scan_fat` walks universal binaries using both `PMA_FAT_MAGIC` and `PMA_FAT_MAGIC_64`. It refuses headers claiming more than `PMA_FAT_MAX_ARCHS` slices, which is what keeps Java class files (they share the `0xcafebabe` magic) from being misread. `pma_scan_archive` walks static libraries in BSD `ar` format, including `#1/` long member names, skips the `__.SYMDEF` index and recurses into each object file. `pma_scan_thin` reads a single Mach-O slice in either byte order and either word size.

Inside a slice it pulls imported names from every place they can live:

- `pma_scan_symtab` reads undefined external entries from `LC_SYMTAB`, skipping debug stabs and common symbols.
- `pma_scan_bind` decodes the regular, weak and lazy bind opcode streams from `LC_DYLD_INFO_ONLY`, so binaries with a stripped symbol table still report their imports.
- `pma_scan_chained` reads the import table from `LC_DYLD_CHAINED_FIXUPS`, which is what Xcode 13 and later emits for iOS 15 targets, in all three import formats.
- `pma_emit_cstrings` reads selector names from the `__objc_methname` section, which is how Objective-C and Swift calls such as `systemUptime` and `activeInputModes` show up.

Every offset is checked with `pma_fits` before it is touched. Every string read uses `strnlen` bounded by its table. A malformed command stops that slice and becomes a diagnostic instead of a crash. I ran the C layer under AddressSanitizer against well over a hundred thousand truncated and bit flipped copies of real binaries with no faults. Nesting is capped at `PMA_MAX_NESTING` so a hostile archive cannot recurse forever.

Two situations get called out instead of silently producing a clean result. If a slice carries `LC_ENCRYPTION_INFO` with a non zero crypt id, the selector section is FairPlay encrypted and the tool says so, because an App Store download cannot be fully checked. If a static library member is LLVM bitcode from an LTO build, `pma_is_bitcode` catches it and you get a `PMA401` warning, because nothing inside it can be read.

**Matching APIs.** `kPMARules` is the catalog. Each `PMARule` names a category, a match kind (`PMAMatchSymbol`, `PMAMatchClass`, `PMAMatchSelector` or `PMAMatchSwift`), the pattern, a readable label and whether it is a heuristic. `pma_match` normalises each name: `_OBJC_CLASS_$_NSUserDefaults` becomes a class reference and a leading underscore is dropped. Suffixes such as `$INODE64` on `stat` are cut off. Swift symbols are matched by their mangled context, so `URLResourceValues.volumeTotalCapacity` is found even though the app never touches `NSURLVolumeTotalCapacityKey` itself.

Rules are split by confidence. Apple's published list is marked direct and produces errors. Indirect paths that almost always mean the same thing, such as `CFPreferencesCopyAppValue` or SwiftUI `@AppStorage`, are heuristics and produce warnings. The `getattrlist` family is special: Apple lists it under both FileTimestamp and DiskSpace, so those rules carry an `alternate` category and are satisfied by either one. If neither is declared you get `PMA103`.

**Deciding who owns what.** `ownerOf:forBinary:kind:` assigns every binary and every manifest to its nearest enclosing bundle: `.app`, `.appex`, `.framework`, `.xpc` or a slice folder of an `.xcframework`. Resource bundles without code, such as the `FirebaseCore_Privacy.bundle` folders CocoaPods creates for static SDKs, pass through to the bundle that contains them, so their manifests count toward the app. Symlinks are never followed and files are deduplicated by inode, so a macOS framework's `Versions/Current` links do not double count. `.dSYM`, `SwiftSupport`, `BCSymbolMaps` and `_CodeSignature` folders are skipped.

**Validating manifests.** `loadManifest:relative:into:` parses XML or binary property lists and checks every rule Apple enforces. Reason codes are looked up in `kPMAReasons`. A code from another category gives `PMA205`, an unknown code gives `PMA204` with a hint when it only differs by case or whitespace, an empty reasons array gives `PMA206` and a misspelt category gives `PMA203` with the correct spelling. It also flags tracking turned on with no tracking domains, domains listed with tracking off, collected data types not in `kPMADataTypes` and purposes not in `kPMAPurposes`. A category whose reasons are all invalid does not count as declared, because Apple will not count it either.

**Evaluating.** `evaluate` merges the valid declarations for each bundle with `declaredFor:` and compares them with what its binaries use. Missing direct categories are `PMA101` errors, missing heuristic ones are `PMA102` warnings. Declared categories with no matching API are `PMA212` notes, because they may be fine (a call through `dlsym` or a script runtime) but are worth a look. Any framework whose name appears in `kPMACommonSDKs` and has no manifest is a `PMA301` error, the ITMS-91061 case.

**Reporting.** The text report is sorted by severity, then path, then code, using `PMACompareFindings`. `PMAJSONAppend` writes JSON with sorted keys so CI diffs stay stable between runs. `skeletonText` prints a ready to paste `NSPrivacyAccessedAPITypes` block for every bundle with gaps, listing each valid reason code with a one line summary inside an XML comment. You still choose the reason: the tool will not guess why your code reads the boot time.

## Usage

Build on macOS with the system compiler:

```sh
clang -fobjc-arc -O2 -framework Foundation PrivacyManifestAuditor.m -o PrivacyManifestAuditor
```

Build on Linux with GNUstep, which is handy for CI runners that only inspect artifacts:

```sh
clang -fobjc-runtime=gcc -I/usr/lib/gcc/x86_64-linux-gnu/13/include \
  $(gnustep-config --objc-flags) PrivacyManifestAuditor.m -o PrivacyManifestAuditor \
  $(gnustep-config --base-libs)
```

Audit an archive in CI and fail the job on errors:

```sh
./PrivacyManifestAuditor --quiet build/MyApp.xcarchive
```

Check a vendor SDK before you adopt it, as JSON:

```sh
./PrivacyManifestAuditor --json Vendor/Analytics.xcframework > analytics-privacy.json
```

Print manifest snippets for everything that is missing:

```sh
./PrivacyManifestAuditor --skeleton build/MyApp.xcarchive
```

Check an IPA you downloaded from App Store Connect. The tool refuses `.ipa` files on purpose, so extract it first:

```sh
unzip -q MyApp.ipa -d extracted
./PrivacyManifestAuditor extracted/Payload/MyApp.app
```

Check a static library you are about to ship, with its manifest kept elsewhere in the repo:

```sh
./PrivacyManifestAuditor --manifest Sources/MySDK/Resources/PrivacyInfo.xcprivacy build/libMySDK.a
```

A finding looks like this:

```
error   PMA101  MyApp.app/Frameworks/Alamofire.framework
        Uses DiskSpace APIs but this bundle ships no PrivacyInfo.xcprivacy, so nothing declares NSPrivacyAccessedAPICategoryDiskSpace (ITMS-91053)
        - MyApp.app/Frameworks/Alamofire.framework/Alamofire [arm64]: statfs()
```

Options:

| Flag | Effect |
|---|---|
| `--json` | Full report with bundles, binaries, detected APIs, manifests and findings |
| `--skeleton` | Paste ready `NSPrivacyAccessedAPITypes` snippets per bundle |
| `--union` | Let any manifest in the scan cover every binary, for a lenient first pass |
| `--no-heuristics` | Report only APIs on Apple's published list |
| `--strict` | Exit 1 on warnings as well as errors |
| `--quiet` | Hide notes in the text report |
| `--manifest <file>` | Attach an extra manifest to the root bundle, repeatable |

Exit status is `0` when clean, `1` when there are errors (or warnings under `--strict`) and `2` for usage or input problems.

## Notes

- Finding codes: `PMA101` to `PMA103` are missing declarations, `PMA201` to `PMA212` are manifest problems, `PMA301` is an SDK on Apple's list without a manifest, `PMA401` and `PMA402` are binaries the tool could only partly read.
- The default scope is per bundle, which matches what Apple asks of SDK authors. `--union` is useful when you want to know whether the app as a whole declares a category, but it will hide a vendor framework that should carry its own manifest.
- Selector matching reads every name in `__objc_methname`, which includes selectors a binary defines as well as ones it calls. In practice the selectors on the list are rarely redefined, so this is the right trade.
- Calls made through `dlsym`, JavaScript bridges, Lua or other script runtimes leave no import behind and cannot be seen. If you use those, declare the category by hand and expect a `PMA212` note.
- Encrypted App Store binaries keep their imports readable but their selectors are encrypted. Scan the archive you uploaded, not the app you downloaded, for full results.
- The SDK list in `kPMACommonSDKs` and the reason codes in `kPMAReasons` follow Apple's published documentation as of late 2026. When Apple adds codes or SDK names, update those two tables. Nothing else depends on them.
- The tool does not verify SDK code signatures. Xcode signs embedded frameworks when it archives, so the signature you would see in a built app belongs to you, not the vendor.
- The source builds under ARC on Apple platforms and under manual reference counting with GNUstep. It uses no blocks, no subscripting and explicit ivars so both toolchains accept it unchanged.
