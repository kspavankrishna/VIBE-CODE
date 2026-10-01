module CacheRules
  ( Severity(..)
  , Finding
  , Context
  , Tool
  , auditDockerfile
  , installTool
  , refsIn
  , declaredArgs
  , isBuildStep
  , isAptRun
  , runWeight
  , severityLabel
  , parseSeverity
  ) where

import Prelude

import Data.Array as Array
import Data.Array.NonEmpty as NEA
import Data.Foldable (any)
import Data.Int as Int
import Data.Maybe (Maybe(..), fromMaybe, isJust)
import Data.String as Str
import Data.String.CodeUnits as CU
import Data.String.Regex as Rx
import Data.String.Regex.Flags (global, ignoreCase, noFlags)
import Data.String.Regex.Unsafe (unsafeRegex)
import DockerfileParser (Dockerfile, Instruction, Stage, flagValue, hasFlag, mountFrom, resolveStage, sourcesOf, stageDeps, words)
import PathGlob (dockerignoreIgnores)

data Severity = Info | Warn | Error

derive instance eqSeverity :: Eq Severity
derive instance ordSeverity :: Ord Severity

severityLabel :: Severity -> String
severityLabel = case _ of
  Info -> "info"
  Warn -> "warn"
  Error -> "error"

parseSeverity :: String -> Maybe Severity
parseSeverity s = case Str.toLower s of
  "info" -> Just Info
  "warn" -> Just Warn
  "error" -> Just Error
  _ -> Nothing

type Finding =
  { id :: String
  , severity :: Severity
  , line :: Int
  , stage :: Int
  , message :: String
  , fix :: String
  }

type Context = { dockerignore :: Maybe (Array String) }

type Tool =
  { name :: String
  , hint :: String
  , copyFiles :: String
  , cacheDir :: String
  }

rx :: String -> Rx.Regex
rx p = unsafeRegex p noFlags

rxi :: String -> Rx.Regex
rxi p = unsafeRegex p ignoreCase

type ToolRule = { re :: Rx.Regex, tool :: Tool }

toolRules :: Array ToolRule
toolRules =
  [ { re: rx "\\b(npm\\s+(ci|install|i|clean-install)|yarn\\s+install|pnpm\\s+(install|i)|bun\\s+install)\\b"
    , tool: { name: "node packages", hint: "package.json and the lockfile (package-lock.json, yarn.lock or pnpm-lock.yaml)", copyFiles: "package.json package-lock.json*", cacheDir: "/root/.npm" }
    }
  , { re: rx "\\b(pip3?\\s+install|python3?\\s+-m\\s+pip\\s+install|uv\\s+(pip\\s+install|sync)|poetry\\s+install|pipenv\\s+(install|sync)|pdm\\s+install)\\b"
    , tool: { name: "python packages", hint: "requirements.txt, or pyproject.toml plus the lockfile", copyFiles: "requirements.txt", cacheDir: "/root/.cache/pip" }
    }
  , { re: rx "\\bgo\\s+mod\\s+download\\b"
    , tool: { name: "go modules", hint: "go.mod and go.sum", copyFiles: "go.mod go.sum", cacheDir: "/go/pkg/mod" }
    }
  , { re: rx "\\bcargo\\s+(fetch|vendor)\\b"
    , tool: { name: "cargo crates", hint: "Cargo.toml and Cargo.lock", copyFiles: "Cargo.toml Cargo.lock", cacheDir: "/usr/local/cargo/registry" }
    }
  , { re: rx "\\bbundle\\s+install\\b"
    , tool: { name: "ruby gems", hint: "Gemfile and Gemfile.lock", copyFiles: "Gemfile Gemfile.lock", cacheDir: "/usr/local/bundle" }
    }
  , { re: rx "\\bcomposer\\s+install\\b"
    , tool: { name: "composer packages", hint: "composer.json and composer.lock", copyFiles: "composer.json composer.lock", cacheDir: "/root/.composer/cache" }
    }
  , { re: rx "\\bmvnw?\\b.*\\bdependency:(go-offline|resolve|resolve-plugins)\\b"
    , tool: { name: "maven dependencies", hint: "pom.xml", copyFiles: "pom.xml", cacheDir: "/root/.m2" }
    }
  , { re: rx "\\bgradlew?\\s+(\\S+\\s+)*dependencies\\b"
    , tool: { name: "gradle dependencies", hint: "build.gradle, settings.gradle and gradle.lockfile", copyFiles: "build.gradle settings.gradle", cacheDir: "/root/.gradle" }
    }
  , { re: rx "\\b(dotnet|nuget)\\s+restore\\b"
    , tool: { name: "nuget packages", hint: "the .csproj or .sln files and nuget.config", copyFiles: "*.csproj nuget.config", cacheDir: "/root/.nuget/packages" }
    }
  , { re: rx "\\bmix\\s+deps\\.get\\b"
    , tool: { name: "hex packages", hint: "mix.exs and mix.lock", copyFiles: "mix.exs mix.lock", cacheDir: "/root/.hex" }
    }
  , { re: rx "\\b(dart|flutter)\\s+pub\\s+get\\b"
    , tool: { name: "pub packages", hint: "pubspec.yaml and pubspec.lock", copyFiles: "pubspec.yaml pubspec.lock", cacheDir: "/root/.pub-cache" }
    }
  ]

editableInstallRe :: Rx.Regex
editableInstallRe = rx "\\bpip3?\\s+install\\s+(-e\\s+|--editable\\s+)?\\.(\\s|$)"

installTool :: Instruction -> Maybe Tool
installTool i
  | i.keyword /= "RUN" = Nothing
  | Rx.test editableInstallRe i.args = Nothing
  | otherwise = map _.tool (Array.find (\r -> Rx.test r.re i.args) toolRules)

buildRe :: Rx.Regex
buildRe = rx "\\b(npm|yarn|pnpm|bun)\\s+(run\\s+)?build\\b|\\bgo\\s+(build|install)\\b|\\bcargo\\s+(build|install)\\b|\\b(mvnw?|gradlew?)\\b.*\\b(package|install|build|assemble)\\b|\\bdotnet\\s+(build|publish)\\b|\\b(make|cmake|ninja|tsc|webpack|rollup|bazel)\\b|\\b(vite|next|nuxt)\\s+build\\b|\\b(gcc|g\\+\\+|clang|rustc|javac)\\b|\\bpython3?\\s+setup\\.py\\b|\\bpip3?\\s+wheel\\b"

isBuildStep :: Instruction -> Boolean
isBuildStep i = i.keyword == "RUN" && Rx.test buildRe i.args

aptRe :: Rx.Regex
aptRe = rx "\\b(apt|apt-get|apk|dnf|yum|microdnf|zypper)\\b"

isAptRun :: Instruction -> Boolean
isAptRun i = i.keyword == "RUN" && Rx.test aptRe i.args

runWeight :: Instruction -> Int
runWeight i = case i.keyword of
  "RUN"
    | isJust (installTool i) -> 10
    | isBuildStep i -> 8
    | isAptRun i -> 6
    | otherwise -> 2
  "COPY" -> 1
  "ADD" -> 1
  _ -> 0

mk :: String -> Severity -> Stage -> Instruction -> String -> String -> Finding
mk id severity st i message fix = { id, severity, line: i.line, stage: st.index, message, fix }

indexed :: forall a. Array a -> Array { i :: Int, x :: a }
indexed = Array.mapWithIndex (\i x -> { i, x })

isLocalCopy :: Instruction -> Boolean
isLocalCopy i = (i.keyword == "COPY" || i.keyword == "ADD") && not (hasFlag "from" i)

isBroadSrc :: String -> Boolean
isBroadSrc s = s == "" || s == "*" || s == "**"

isBroadCopy :: Instruction -> Boolean
isBroadCopy i = isLocalCopy i && any isBroadSrc (sourcesOf i).sources

hasCacheMount :: Instruction -> Boolean
hasCacheMount i = any (\f -> f.name == "mount" && Str.contains (Str.Pattern "type=cache") f.value) i.flags

runs :: Stage -> Array { i :: Int, x :: Instruction }
runs st = Array.filter (\r -> r.x.keyword == "RUN") (indexed st.body)

cmdParts :: String -> Array String
cmdParts s = Array.filter (not <<< Str.null) (map Str.trim (Rx.split (rx "&&|\\|\\||;|\\||\\n") s))

showInt :: Int -> String
showInt = show

installTemplate :: Tool -> String
installTemplate t = "COPY " <> t.copyFiles <> " ./"

broadCopyBeforeInstall :: Context -> Dockerfile -> Stage -> Array Finding
broadCopyBeforeInstall _ _ st = Array.mapMaybe check (runs st)
  where
  check r = case installTool r.x of
    Nothing -> Nothing
    Just tool -> case Array.find isBroadCopy (Array.take r.i st.body) of
      Nothing -> Nothing
      Just bc -> Just $ mk "DCA001" Error st r.x
        ( "COPY on line " <> showInt bc.line <> " brings the whole build context in before the " <> tool.name <> " install on line " <> showInt r.x.line <> ". Any edit to any source file invalidates the install layer and every dependency is downloaded again."
        )
        ( "Move the install above the broad COPY and copy only " <> tool.hint <> " first: `" <> installTemplate tool <> "` then the install RUN, then `COPY . .`."
        )

baseStage :: Dockerfile -> Stage -> Maybe Stage
baseStage d s = Array.find (\o -> o.index < s.index && map Str.toLower o.name == Just (Str.toLower s.image)) d.stages

fromPinning :: Context -> Dockerfile -> Stage -> Array Finding
fromPinning _ df st
  | st.image == "" = []
  | Str.toLower st.image == "scratch" = []
  | isJust (baseStage df st) = []
  | Str.contains (Str.Pattern "$") st.image =
      [ mk "DCA002" Info st st.from
          ("Base image `" <> st.image <> "` comes from a build argument, so this audit cannot tell whether it is pinned.")
          "Give the argument a default that includes a digest, or pass the digest from CI."
      ]
  | Str.contains (Str.Pattern "@sha256:") st.image = []
  | otherwise = case tagOf st.image of
      Nothing ->
        [ mk "DCA002" Warn st st.from
            ("Base image `" <> st.image <> "` has no tag, so it resolves to latest. A new upstream push silently invalidates every layer below and changes what you ship.")
            "Pin a version tag and a digest, for example `name:1.2.3@sha256:<digest>`, and let a bot bump it."
        ]
      Just "latest" ->
        [ mk "DCA002" Warn st st.from
            ("Base image `" <> st.image <> "` uses the latest tag. The cache key moves whenever the upstream tag moves.")
            "Pin a version tag and a digest, and let a bot bump it."
        ]
      Just _ ->
        [ mk "DCA002" Info st st.from
            ("Base image `" <> st.image <> "` is tagged but not pinned by digest. Tags are mutable, so two builds of the same commit can differ.")
            "Append `@sha256:<digest>` and keep the tag for readability."
        ]
  where
  tagOf img =
    let
      lastSeg = fromMaybe img (Array.last (Str.split (Str.Pattern "/") img))
    in
      case Str.split (Str.Pattern ":") lastSeg of
        [ _, t ] -> Just t
        _ -> Nothing

aptRules :: Context -> Dockerfile -> Stage -> Array Finding
aptRules _ _ st = Array.concatMap perRun (runs st)
  where
  updateRe = rx "\\bapt(-get)?\\s+([^&;|]*\\s)?update\\b"
  installRe = rx "\\bapt(-get)?\\s+([^&;|]*\\s)?install\\b"
  yesRe = rx "(\\s-[a-zA-Z]*y[a-zA-Z]*\\b|--yes|--assume-yes|-qq)"
  recommendsRe = rx "--no-install-recommends|Install-Recommends=(false|0)"
  cleanRe = rx "rm\\s+(-\\S+\\s+)*/var/lib/apt/lists|apt(-get)?\\s+clean"
  apkRe = rx "\\bapk\\s+([^&;|]*\\s)?add\\b"
  apkCleanRe = rx "--no-cache|/var/cache/apk"
  rpmRe = rx "\\b(dnf|yum|microdnf)\\s+([^&;|]*\\s)?install\\b"
  rpmCleanRe = rx "clean\\s+all|/var/cache/(dnf|yum)|keepcache=0|keepcache=false"

  installLater r = any (\o -> o.i > r.i && Rx.test installRe o.x.args && not (Rx.test updateRe o.x.args)) (runs st)

  perRun r =
    let
      t = r.x.args
      mounted = hasCacheMount r.x
      upd = Rx.test updateRe t
      ins = Rx.test installRe t
    in
      Array.concat
        [ if upd && not ins then
            [ if installLater r then
                mk "DCA003" Error st r.x
                  "apt-get update runs in its own layer and a later RUN installs packages. Once the update layer is cached, the install uses a stale package index and fails with 404 errors on packages that were rotated out of the mirror."
                  "Run `apt-get update && apt-get install -y --no-install-recommends ...` in one RUN."
              else
                mk "DCA003" Warn st r.x
                  "apt-get update has no install in the same RUN. The index is frozen into a layer that nothing uses and later installs will see it stale."
                  "Merge it into the RUN that installs packages."
            ]
          else []
        , if ins && not (Rx.test yesRe t) then
            [ mk "DCA003" Error st r.x
                "apt-get install without -y waits for a prompt that never arrives and the build aborts."
                "Add -y and set DEBIAN_FRONTEND=noninteractive for the RUN only."
            ]
          else []
        , if ins && not (Rx.test recommendsRe t) then
            [ mk "DCA003" Warn st r.x
                "apt-get install pulls recommended packages. That often doubles the layer and widens the set of things that can change under you."
                "Add --no-install-recommends and list what you actually need."
            ]
          else []
        , if ins && not (Rx.test cleanRe t) && not mounted then
            [ mk "DCA003" Warn st r.x
                "The apt package index stays in this layer. That is tens of megabytes of dead weight in every image and every cache export."
                "End the RUN with `rm -rf /var/lib/apt/lists/*`, or use a cache mount on /var/cache/apt and /var/lib/apt."
            ]
          else []
        , if Rx.test apkRe t && not (Rx.test apkCleanRe t) && not mounted then
            [ mk "DCA003" Warn st r.x
                "apk add keeps its index cache in the layer."
                "Use `apk add --no-cache`."
            ]
          else []
        , if Rx.test rpmRe t && not (Rx.test rpmCleanRe t) && not mounted then
            [ mk "DCA003" Warn st r.x
                "The dnf or yum metadata cache stays in this layer."
                "End the RUN with `dnf clean all && rm -rf /var/cache/dnf`, or use a cache mount."
            ]
          else []
        ]

pipRe :: Rx.Regex
pipRe = rx "\\b(pip3?|python3?\\s+-m\\s+pip)\\s+install\\b"

packageManagerRules :: Context -> Dockerfile -> Stage -> Array Finding
packageManagerRules _ df st = Array.concatMap perRun (runs st)
  where
  envNoCache = any (\i -> i.keyword == "ENV" && Str.contains (Str.Pattern "PIP_NO_CACHE_DIR") i.args) (st.body <> df.preamble)

  perRun r =
    let
      t = r.x.args
      parts = map words (cmdParts t)
    in
      Array.concat
        [ if Rx.test pipRe t && not (Str.contains (Str.Pattern "--no-cache-dir") t) && not envNoCache && not (hasCacheMount r.x) then
            [ mk "DCA004" Warn st r.x
                "pip install writes its wheel cache into the layer. It never helps a later build because layer caching already covers repeat installs."
                "Add --no-cache-dir, set PIP_NO_CACHE_DIR=1, or mount a pip cache with `RUN --mount=type=cache,target=/root/.cache/pip`."
            ]
          else []
        , if any bareNpmInstall parts then
            [ mk "DCA004" Warn st r.x
                "npm install can rewrite the lockfile and resolve newer versions than the ones you tested. The layer content depends on the registry at build time."
                "Use `npm ci` so the build fails when package.json and the lockfile disagree."
            ]
          else []
        , if any (unfrozen "yarn" [ "--frozen-lockfile", "--immutable" ]) parts then
            [ mk "DCA004" Info st r.x
                "yarn install without --frozen-lockfile or --immutable is free to update the lockfile during the build."
                "Add --frozen-lockfile for yarn 1 or --immutable for yarn berry."
            ]
          else []
        , if any (unfrozen "pnpm" [ "--frozen-lockfile" ]) parts then
            [ mk "DCA004" Info st r.x
                "pnpm install outside CI can update the lockfile."
                "Add --frozen-lockfile."
            ]
          else []
        ]

  bareNpmInstall ws = case Array.uncons ws of
    Just { head: "npm", tail } -> case Array.uncons tail of
      Just { head: sub, tail: rest } ->
        (sub == "install" || sub == "i")
          && all' (\w -> CU.take 1 w == "-") rest
          && not (any (\w -> w == "-g" || w == "--global") rest)
      Nothing -> false
    _ -> false

  unfrozen tool needles ws = case Array.uncons ws of
    Just { head, tail } | head == tool -> case Array.uncons tail of
      Just { head: "install", tail: rest } -> not (any (\n -> Array.elem n rest) needles)
      _ -> false
    _ -> false

  all' p = not <<< any (not <<< p)

addRules :: Context -> Dockerfile -> Stage -> Array Finding
addRules _ _ st = Array.concatMap check st.body
  where
  archiveRe = rxi "\\.(tar|tar\\.gz|tgz|tar\\.xz|txz|tar\\.bz2|tbz2|tar\\.zst)$"
  urlRe = rxi "^(https?://|git@|git://)"

  check i
    | i.keyword /= "ADD" || hasFlag "from" i = []
    | otherwise =
        let
          srcs = (sourcesOf i).sources
          remote = Array.filter (Rx.test urlRe) srcs
          local = Array.filter (\s -> not (Rx.test urlRe s) && not (Rx.test archiveRe s)) srcs
        in
          Array.concat
            [ if not (Array.null remote) && not (hasFlag "checksum" i) then
                [ mk "DCA005" Warn st i
                    "ADD fetches a remote URL with no checksum. The layer cache keys on HTTP metadata and nothing verifies what came down."
                    "Add `--checksum=sha256:<digest>`, or download in a RUN and verify it yourself."
                ]
              else []
            , if not (Array.null local) && Array.null remote then
                [ mk "DCA005" Info st i
                    "ADD with a plain local path behaves like COPY but can also unpack archives and fetch URLs, which surprises people."
                    "Use COPY unless you want ADD's extraction."
                ]
              else []
            ]

volatileRe :: Rx.Regex
volatileRe = unsafeRegex "^(BUILD_?(DATE|TIME|TIMESTAMP|NUMBER|ID|URL|VERSION|REVISION)|GIT_?(SHA|COMMIT|REV|REF|HASH|BRANCH|TAG)|VCS_?REF|COMMIT(_?SHA|_?HASH)?|SOURCE_COMMIT|CI_[A-Z0-9_]+|GITHUB_[A-Z0-9_]+|RUN_(ID|NUMBER|ATTEMPT)|TIMESTAMP|REVISION|CREATED|NOW)$" ignoreCase

varRefRe :: Rx.Regex
varRefRe = unsafeRegex "\\$\\{?([A-Za-z_][A-Za-z0-9_]*)" global

varRefOneRe :: Rx.Regex
varRefOneRe = rx "\\$\\{?([A-Za-z_][A-Za-z0-9_]*)"

refsIn :: String -> Array String
refsIn s = case Rx.match varRefRe s of
  Nothing -> []
  Just m -> Array.mapMaybe one (map (fromMaybe "") (NEA.toArray m))
  where
  one v = do
    g <- Rx.match varRefOneRe v
    join (NEA.index g 1)

declaredArgs :: Instruction -> Array String
declaredArgs i = map (\w -> fromMaybe w (map (\k -> CU.take k w) (CU.indexOf (Str.Pattern "=") w))) (words i.args)

volatileRules :: Context -> Dockerfile -> Stage -> Array Finding
volatileRules _ _ st = Array.mapMaybe check (indexed st.body)
  where
  isHeavy i = isJust (installTool i) || isBuildStep i || isAptRun i

  culprit i = case i.keyword of
    "ARG" -> Array.find (Rx.test volatileRe) (declaredArgs i)
    "LABEL" -> Array.find (Rx.test volatileRe) (refsIn i.args)
    "ENV" -> Array.find (Rx.test volatileRe) (refsIn i.args)
    _ -> Nothing

  check r = case culprit r.x of
    Nothing -> Nothing
    Just name ->
      let
        later = Array.filter isHeavy (Array.drop (r.i + 1) st.body)
      in
        if Array.null later then Nothing
        else Just $ mk "DCA006" Warn st r.x
          ( "`" <> name <> "` changes on every build and is set before " <> showInt (Array.length later) <> " expensive RUN instruction" <> (if Array.length later == 1 then "" else "s") <> ". Docker documents that a changed build argument is a cache miss at its first use, and every RUN after an ARG counts as using it. A LABEL or ENV that expands it changes the image config that every later layer builds on."
          )
          "Declare it in the last stage after the expensive layers, or pass it as a label at build time with `--label` so no instruction in the Dockerfile depends on it."

deleteRules :: Context -> Dockerfile -> Stage -> Array Finding
deleteRules _ _ st = Array.mapMaybe check (indexed st.body)
  where
  deleteRe = rx "^(rm\\s|apt(-get)?\\s+(clean|autoclean|autoremove)|apk\\s+del|yum\\s+clean|dnf\\s+clean)"

  check r
    | r.x.keyword /= "RUN" = Nothing
    | Str.contains (Str.Pattern "<<") r.x.args = Nothing
    | not (any (\p -> p.x.keyword == "RUN" || isLocalCopy p.x || p.x.keyword == "ADD") (indexed (Array.take r.i st.body))) = Nothing
    | otherwise =
        let
          parts = cmdParts r.x.args
        in
          if not (Array.null parts) && all' (Rx.test deleteRe) parts then
            Just $ mk "DCA007" Warn st r.x
              "This RUN only deletes files. The files still live in the earlier layer, so the image does not shrink and the cleanup is a wasted layer."
              "Delete in the same RUN that created the files, or move the work into a build stage and copy only the result."
          else Nothing

  all' p = not <<< any (not <<< p)

singleStageBuild :: Context -> Dockerfile -> Stage -> Array Finding
singleStageBuild _ df st
  | Array.length df.stages /= 1 = []
  | Str.toLower st.image == "scratch" = []
  | otherwise = case Array.find isBuildStep st.body of
      Nothing -> []
      Just b ->
        [ mk "DCA008" Info st b
            "The only stage compiles code and also ships the result, so the toolchain, sources and build caches end up in the runtime image."
            "Split into a build stage and a runtime stage and `COPY --from=build` only the artifact."
        ]

stageGraphRules :: Context -> Dockerfile -> Stage -> Array Finding
stageGraphRules _ df st = Array.concat [ duplicate, Array.concatMap refCheck st.body, dead ]
  where
  lower = map Str.toLower
  duplicate = case st.name of
    Nothing -> []
    Just n ->
      if Array.elem (Str.toLower n) (lower (Array.catMaybes (map _.name (Array.take st.index df.stages)))) then
        [ mk "DCA009" Error st st.from
            ("Stage name `" <> n <> "` is already used by an earlier stage. Docker resolves `--from` to the first match, so this stage can never be referenced.")
            "Rename one of the stages."
        ]
      else []

  looksExternal r = any (\c -> Str.contains (Str.Pattern c) r) [ "/", ":", "." ]

  refCheck i = Array.concatMap (checkRef i) (Array.catMaybes [ flagValue "from" i ] <> mountFrom i)

  checkRef i ref = case resolveStage df ref of
    Just n
      | n >= st.index ->
          [ mk "DCA009" Error st i
              ("`--from=" <> ref <> "` points at stage " <> showInt n <> ", which is not built before stage " <> showInt st.index <> ".")
              "Reorder the stages so the source stage comes first."
          ]
      | otherwise -> []
    Nothing ->
      if isJust (Int.fromString ref) then
        [ mk "DCA009" Error st i
            ("`--from=" <> ref <> "` is not a valid stage index.")
            "Use a stage name or an index below the current stage."
        ]
      else if looksExternal ref then []
      else
        [ mk "DCA009" Warn st i
            ("No stage is named `" <> ref <> "`, so Docker will try to pull an image with that name.")
            "Fix the stage name, or write the full image reference if the pull is intended."
        ]

  usedElsewhere = any (\o -> Array.elem st.index (stageDeps df o)) df.stages
  isLast = st.index == Array.length df.stages - 1
  dead =
    if isLast || usedElsewhere then []
    else
      [ mk "DCA009" Info st st.from
          ("Stage " <> showInt st.index <> maybe' st.name <> " is never used by a later stage. BuildKit skips it for the default target, so it is either dead code or a manual `--target`.")
          "Delete it, or keep it and document the target name."
      ]

  maybe' = case _ of
    Just n -> " (`" <> n <> "`)"
    Nothing -> ""

cacheMountRules :: Context -> Dockerfile -> Stage -> Array Finding
cacheMountRules _ _ st = Array.mapMaybe check (runs st)
  where
  check r = case installTool r.x of
    Just tool | not (hasCacheMount r.x) ->
      Just $ mk "DCA010" Info st r.x
        ("The " <> tool.name <> " install has no BuildKit cache mount. When the layer is invalidated, every package is downloaded again from the network.")
        ("Use `RUN --mount=type=cache,target=" <> tool.cacheDir <> " ...` so a rebuilt layer reuses downloads. Adjust the path if the build runs as another user.")
    _ -> Nothing

contextRules :: Context -> Dockerfile -> Stage -> Array Finding
contextRules ctx _ st = case Array.find isBroadCopy st.body of
  Nothing -> []
  Just bc -> case ctx.dockerignore of
    Nothing ->
      [ mk "DCA011" Warn st bc
          "Broad COPY with no .dockerignore. The whole directory, including .git, local env files and build output, is hashed into this layer and sent to the daemon."
          "Add a .dockerignore next to the Dockerfile or build context root with at least .git, .env* and your dependency and build folders."
      ]
    Just pats ->
      Array.catMaybes
        [ if dockerignoreIgnores pats ".git/HEAD" then Nothing
          else Just $ mk "DCA011" Warn st bc
            "The .dockerignore does not exclude .git. Every commit changes files under .git, so this COPY misses the cache on every commit even when no source changed."
            "Add `.git` to .dockerignore."
        , if dockerignoreIgnores pats ".env" then Nothing
          else Just $ mk "DCA011" Warn st bc
            "The .dockerignore does not exclude .env. A broad COPY bakes that file into a layer that anyone with the image can read."
            "Add `.env` and `.env.*` to .dockerignore."
        ]

stageRules :: Array (Context -> Dockerfile -> Stage -> Array Finding)
stageRules =
  [ broadCopyBeforeInstall
  , fromPinning
  , aptRules
  , packageManagerRules
  , addRules
  , volatileRules
  , deleteRules
  , singleStageBuild
  , stageGraphRules
  , cacheMountRules
  , contextRules
  ]

auditDockerfile :: Context -> Dockerfile -> Array Finding
auditDockerfile ctx df =
  Array.sortBy cmp (Array.concatMap (\st -> Array.concatMap (\r -> r ctx df st) stageRules) df.stages)
  where
  cmp a b = compare a.line b.line <> compare a.id b.id
