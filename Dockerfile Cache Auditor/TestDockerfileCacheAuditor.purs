module TestDockerfileCacheAuditor (main) where

import Prelude

import CacheInvalidation (planInvalidation)
import CacheRules (Severity(..), auditDockerfile)
import Data.Array as Array
import Data.Either (Either(..), isLeft)
import Data.Foldable (for_)
import Data.Maybe (Maybe(..), isJust)
import Data.String as Str
import DockerfileCacheAuditor (audit, parseArgs, renderJson, renderText)
import DockerfileParser (parseDockerfile, sourcesOf)
import Effect (Effect)
import Effect.Console (log)
import Effect.Exception (throw)
import PathGlob (contextMatches, dockerignoreIgnores)

type Case = { name :: String, ok :: Boolean }

ids :: Maybe (Array String) -> String -> Array String
ids di src = map _.id (auditDockerfile { dockerignore: di } (parseDockerfile src))

hasId :: String -> Array String -> Boolean
hasId = Array.elem

broadBeforeInstall :: String
broadBeforeInstall = """FROM python:3.12-slim
WORKDIR /app
COPY . .
RUN pip install --no-cache-dir -r requirements.txt
"""

manifestFirst :: String
manifestFirst = """FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
"""

continuation :: String
continuation = """# escape=`
FROM mcr.microsoft.com/windows/servercore:ltsc2022
RUN echo one `
    # a comment in the middle
    && echo two
COPY a.txt b.txt C:\dest\
"""

heredoc :: String
heredoc = """FROM debian:12
RUN <<EOF
apt-get update
apt-get install -y --no-install-recommends curl
rm -rf /var/lib/apt/lists/*
EOF
COPY <<-CONF /etc/app.conf
	key=value
	CONF
CMD ["true"]
"""

twoStage :: String
twoStage = """FROM golang:1.23 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /out/app ./cmd/app

FROM gcr.io/distroless/static AS runtime
COPY --from=build /out/app /app
"""

forwardRef :: String
forwardRef = """FROM alpine AS one
COPY --from=two /x /x
FROM alpine AS two
FROM alpine AS two
COPY --from=missing /y /y
"""

cases :: Array Case
cases =
  let
    parsed = parseDockerfile continuation
    heredocParsed = parseDockerfile heredoc
    stageCount = Array.length (parseDockerfile twoStage).stages
    plan paths changedArgs src target =
      planInvalidation (parseDockerfile src)
        { paths, ignore: [], args: changedArgs, target }
  in
    [ { name: "broad copy before pip install is DCA001"
      , ok: hasId "DCA001" (ids (Just [ ".git", ".env" ]) broadBeforeInstall)
      }
    , { name: "manifest first ordering is clean of DCA001"
      , ok: not (hasId "DCA001" (ids (Just [ ".git", ".env" ]) manifestFirst))
      }
    , { name: "escape directive joins continuation lines and drops inner comments"
      , ok: case Array.index (Array.concatMap _.body parsed.stages) 0 of
          Just i -> i.args == "echo one     && echo two" && i.line == 3 && i.endLine == 5
          Nothing -> false
      }
    , { name: "heredoc RUN keeps its body inside one instruction"
      , ok: case Array.index (Array.concatMap _.body heredocParsed.stages) 0 of
          Just i -> Str.contains (Str.Pattern "apt-get install") i.args && i.endLine == 6
          Nothing -> false
      }
    , { name: "heredoc RUN with cleanup raises no apt warnings"
      , ok: not (hasId "DCA003" (ids (Just []) heredoc))
      }
    , { name: "COPY heredoc has no file sources"
      , ok: case Array.index (Array.concatMap _.body heredocParsed.stages) 1 of
          Just i -> (sourcesOf i).sources == []
          Nothing -> false
      }
    , { name: "two stage go build is clean"
      , ok: stageCount == 2 && not (hasId "DCA001" (ids (Just [ ".git", ".env" ]) twoStage))
      }
    , { name: "stage graph catches forward, duplicate and unknown references"
      , ok:
          let
            found = Array.filter (\f -> f.id == "DCA009" && f.severity >= Warn) (auditDockerfile { dockerignore: Nothing } (parseDockerfile forwardRef))
          in
            Array.length found >= 3
      }
    , { name: "glob matches directory prefixes and double star"
      , ok: contextMatches "src" "src/app/main.ts" && contextMatches "**/*.md" "docs/a/b.md" && not (contextMatches "*.md" "docs/a.md") && contextMatches "." "anything"
      }
    , { name: "dockerignore last match wins and negation re-includes"
      , ok: dockerignoreIgnores [ "docs", "!docs/keep.md" ] "docs/other.md" && not (dockerignoreIgnores [ "docs", "!docs/keep.md" ] "docs/keep.md")
      }
    , { name: "source edit leaves the dependency stage cached"
      , ok: case plan [ "src/main.go" ] [] twoStage Nothing of
          Right p -> p.firstMissLine == Just 5 && Array.length (Array.filter (\s -> isJust s.firstMissLine) p.stages) == 2
          Left _ -> false
      }
    , { name: "manifest edit rebuilds the download layer and everything below"
      , ok: case plan [ "go.mod" ] [] twoStage Nothing of
          Right p -> p.firstMissLine == Just 3 && p.percent == 100
          Left _ -> false
      }
    , { name: "edit to an ignored path predicts zero rebuild"
      , ok: case planInvalidation (parseDockerfile twoStage) { paths: [ "README.md" ], ignore: [ "*.md" ], args: [], target: Nothing } of
          Right p -> p.percent == 0 && p.firstMissLine == Nothing && Array.length p.ignoredPaths == 1
          Left _ -> false
      }
    , { name: "any edit misses at a broad COPY when nothing is ignored"
      , ok: case plan [ "README.md" ] [] twoStage Nothing of
          Right p -> p.firstMissLine == Just 5
          Left _ -> false
      }
    , { name: "volatile build arg before a RUN is a DCA006 warning"
      , ok: hasId "DCA006" (ids Nothing "FROM alpine:3.20\nARG BUILD_DATE\nRUN apk add --no-cache curl\n")
      }
    , { name: "changed build arg misses at the first RUN after ARG"
      , ok: case plan [] [ "BUILD_DATE" ] "FROM alpine:3.20\nRUN echo hi\nARG BUILD_DATE\nRUN echo bye\n" Nothing of
          Right p -> p.firstMissLine == Just 4
          Left _ -> false
      }
    , { name: "unknown target is an error"
      , ok: isLeft (plan [ "a" ] [] twoStage (Just "nope"))
      }
    , { name: "argument parser rejects unknown flags and bad percent"
      , ok: isLeft (parseArgs [ "--wat", "x", "Dockerfile" ]) && isLeft (parseArgs [ "--max-rebuild", "400", "Dockerfile" ])
      }
    , { name: "report renders in both formats"
      , ok: case parseArgs [ "--changed", "x", "Dockerfile" ] of
          Right o -> case audit o (parseDockerfile twoStage) Nothing [ "go.mod" ] of
            Right r ->
              Str.contains (Str.Pattern "predicted rebuild") (renderText "Dockerfile" r)
                && Str.contains (Str.Pattern "\"percent\":100") (renderJson "Dockerfile" r)
            Left _ -> false
          Left _ -> false
      }
    , { name: "json output escapes quotes in strings"
      , ok: case parseArgs [ "Dockerfile" ] of
          Right o -> case audit o (parseDockerfile "FROM alpine:3.20\n") Nothing [] of
            Right r -> Str.contains (Str.Pattern "\"a\\\"b\"") (renderJson "a\"b" r)
            Left _ -> false
          Left _ -> false
      }
    ]

main :: Effect Unit
main = do
  for_ cases \c -> log ((if c.ok then "ok    " else "FAIL  ") <> c.name)
  let
    failed = Array.length (Array.filter (not <<< _.ok) cases)
  when (failed > 0) $ throw (show failed <> " test(s) failed")
  log (show (Array.length cases) <> " tests passed")
