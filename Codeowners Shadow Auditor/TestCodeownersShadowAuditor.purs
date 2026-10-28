module TestCodeownersShadowAuditor (main) where

import Prelude

import CodeownersAnalysis (Severity(..), analyze, lint, ownersOf)
import CodeownersFile (parseCodeowners, validOwner)
import CodeownersPattern (compilePattern, covers, matchesPath, segmentGlobCovers, segmentGlobMatch, splitPath)
import CodeownersShadowAuditor (Format(..), gate, parseArgs, renderJson, renderText)
import Data.Array as Array
import Data.Either (Either(..), isLeft)
import Data.Foldable (for_)
import Data.Maybe (Maybe(..), isNothing)
import Data.String as Str
import Effect (Effect)
import Effect.Console (log)
import Effect.Exception (throw)

type Case = { name :: String, ok :: Boolean }

hit :: String -> String -> Boolean
hit pat path = case compilePattern pat of
  Just p -> matchesPath p (splitPath path)
  Nothing -> false

cov :: String -> String -> Boolean
cov b a = case compilePattern b, compilePattern a of
  Just x, Just y -> covers x y
  _, _ -> false

codes :: String -> Array String
codes src = map _.code (lint (parseCodeowners src))

has :: String -> String -> Boolean
has code src = Array.elem code (codes src)

cases :: Array Case
cases =
  [ { name: "star matches a file at any depth", ok: hit "*.js" "src/a/b.js" && hit "*.js" "b.js" }
  , { name: "star does not cross a slash inside a segment", ok: not (hit "/src/*.js" "src/a/b.js") }
  , { name: "leading slash anchors to the root", ok: hit "/docs/a.md" "docs/a.md" && not (hit "/docs/a.md" "x/docs/a.md") }
  , { name: "pattern without a slash floats", ok: hit "a.md" "x/y/a.md" }
  , { name: "trailing slash owns everything below", ok: hit "/src/" "src/a/b/c.ts" && hit "docs/" "x/docs/a.md" }
  , { name: "trailing slash does not match a plain file of that name", ok: not (hit "docs/" "docs") }
  , { name: "directory wildcard owns direct children only", ok: hit "/docs/*" "docs/a.md" && not (hit "/docs/*" "docs/sub/a.md") }
  , { name: "double star in the middle spans directories", ok: hit "/a/**/z.txt" "a/z.txt" && hit "/a/**/z.txt" "a/b/c/z.txt" }
  , { name: "trailing double star equals trailing slash", ok: hit "/src/**" "src/a/b.ts" && not (hit "/src/**" "src") }
  , { name: "bare double star matches every file", ok: hit "**" "a/b/c" && hit "*" "top.txt" }
  , { name: "slash alone has no segment", ok: isNothing (compilePattern "/") }
  , { name: "glob match handles question mark", ok: segmentGlobMatch "a?c" "abc" && not (segmentGlobMatch "a?c" "ac") }
  , { name: "glob match is linear on hostile input", ok: segmentGlobMatch "*a*a*a*a*a*a*a*a*b" (Str.joinWith "" (Array.replicate 200 "a") <> "b") }
  , { name: "glob cover accepts wider globs", ok: segmentGlobCovers "*.ts" "a*.ts" && segmentGlobCovers "*" "x?y" }
  , { name: "glob cover rejects narrower globs", ok: not (segmentGlobCovers "a*" "*") && not (segmentGlobCovers "?" "*") }
  , { name: "catch all covers a file rule", ok: cov "*" "/src/app.ts" }
  , { name: "src double star covers src directory rule", ok: cov "/src/**" "/src/auth/" && cov "/src/" "/src/auth/" }
  , { name: "narrow directory does not cover its parent", ok: not (cov "/src/auth/" "/src/") }
  , { name: "markdown rule does not cover docs directory", ok: not (cov "*.md" "/docs/") }
  , { name: "catch all file rule covers a whole directory", ok: cov "*" "/src/" }
  , { name: "single level wildcard cannot cover nested files", ok: not (cov "/docs/*" "/docs/api/x.md") }
  , { name: "directory rule covers single level wildcard", ok: cov "/docs/" "/docs/*" }
  , { name: "floating directory covers anchored directory", ok: cov "docs/" "/docs/api/" && not (cov "/docs/" "docs/") }
  , { name: "different extensions are not covered", ok: not (cov "*.ts" "*.js") }
  , { name: "owner syntax", ok: validOwner "@octo" && validOwner "@octo/core-team" && validOwner "a@b.io" && not (validOwner "octo") && not (validOwner "@a/b/c") && not (validOwner "a@b") }
  , { name: "inline comment ends the owner list", ok: map _.owners (parseCodeowners "/a/ @x @y # note @z\n") == [ [ "@x", "@y" ] ] }
  , { name: "escaped hash is a pattern not a comment", ok: map _.text (parseCodeowners "\\#notes/ @x\n") == [ "#notes/" ] && not (has "unsupported-escape" "\\#notes/ @x\n") }
  , { name: "tabs and carriage returns split fields", ok: map _.owners (parseCodeowners "/a/\t@x\r\n") == [ [ "@x" ] ] }
  , { name: "line numbers survive blank lines and comments", ok: map _.line (parseCodeowners "# c\n\n/a/ @x\n\n/b/ @y\n") == [ 3, 5 ] }
  , { name: "invalid owner is an error and the rule is skipped", ok: has "invalid-owner" "/a/ nobody\n" && (analyze (parseCodeowners "/a/ nobody\n") Nothing).effective == 0 }
  , { name: "negation and ranges are flagged", ok: has "unsupported-negation" "!/a/ @x\n" && has "unsupported-range" "/a/[ab].ts @x\n" }
  , { name: "later catch all shadows earlier rules", ok: has "shadowed" "/a/ @x\n/b/ @y\n* @z\n" }
  , { name: "repeated pattern is reported once as a duplicate", ok: has "duplicate-pattern" "/a/ @x\n/a/ @y\n" && not (has "shadowed" "/a/ @x\n/a/ @y\n") }
  , { name: "broad rule after narrow rule shadows the narrow one", ok: has "shadowed" "/src/auth/ @x\n/src/ @y\n" }
  , { name: "narrow rule after broad rule is fine", ok: not (has "shadowed" "/src/ @y\n/src/auth/ @x\n") }
  , { name: "narrower later rule that drops owners is reported", ok: has "owner-override" "/src/ @a @b\n/src/auth/ @a\n" }
  , { name: "keeping the owners silences the override note", ok: not (has "owner-override" "/src/ @a\n/src/auth/ @a @b\n") }
  , { name: "default owner line does not trigger override noise", ok: not (has "owner-override" "* @a\n/src/ @b\n") }
  , { name: "ownerless rule is noted as intentional", ok: has "clears-ownership" "/vendor/\n" }
  , { name: "last match wins for ownership", ok: ownersOf (parseCodeowners "* @a\n/src/ @b\n/src/x.ts @c\n") (splitPath "src/x.ts") == [ "@c" ] }
  , { name: "coverage lists unowned files", ok: map _.unowned (analyze (parseCodeowners "/src/ @a\n") (Just [ "src/a.ts", "README.md" ])).coverage == Just [ "README.md" ] }
  , { name: "unmatched rule is reported against the file list", ok: Array.elem "unmatched-rule" (map _.code (analyze (parseCodeowners "/gone/ @a\n/src/ @b\n") (Just [ "src/a.ts" ])).findings) }
  , { name: "union shadowing shows up as never wins", ok: Array.elem "never-wins" (map _.code (analyze (parseCodeowners "/lib/ @a\n/lib/*.js @b\n/lib/*/ @c\n") (Just [ "lib/f.js", "lib/d/g.ts" ])).findings) && not (has "shadowed" "/lib/ @a\n/lib/*.js @b\n/lib/*/ @c\n") }
  , { name: "unowned count comes from the winning rule", ok: case (analyze (parseCodeowners "* @a\n/vendor/\n") (Just [ "a.ts", "vendor/x.c" ])).coverage of
        Just c -> c.total == 2 && c.owned == 1 && c.unowned == [ "vendor/x.c" ]
        Nothing -> false
    }
  , { name: "args parse", ok: case parseArgs [ "--format", "json", "--fail-on", "warn", "--max-unowned", "3", "--files", "-", "CODEOWNERS" ] of
        Right o -> o.format == Json && o.failOn == Warn && o.maxUnowned == Just 3 && o.files == Just "-" && o.codeowners == "CODEOWNERS"
        Left _ -> false
    }
  , { name: "bad args are rejected", ok: isLeft (parseArgs [ "--nope", "x" ]) && isLeft (parseArgs []) && isLeft (parseArgs [ "--max-unowned", "-1", "f" ]) && isLeft (parseArgs [ "a", "b" ]) }
  , { name: "gate follows fail on and max unowned", ok: case parseArgs [ "--fail-on", "warn", "--max-unowned", "0", "C" ] of
        Right o ->
          not (gate o (analyze (parseCodeowners "/a/ @x\n/a/ @y\n") Nothing))
            && gate o (analyze (parseCodeowners "* @x\n") (Just [ "a" ]))
            && not (gate o (analyze (parseCodeowners "/a/ @x\n") (Just [ "b" ])))
        Left _ -> false
    }
  , { name: "renderers mention every finding", ok: case parseArgs [ "C" ] of
        Right o ->
          let
            report = analyze (parseCodeowners "/a/ @x\n* @y\n") Nothing
          in
            Str.contains (Str.Pattern "shadowed") (renderText o report) && Str.contains (Str.Pattern "\"code\":\"shadowed\"") (renderJson report)
        Left _ -> false
    }
  ]

main :: Effect Unit
main = do
  let failed = Array.filter (not <<< _.ok) cases
  for_ cases \c -> log ((if c.ok then "ok    " else "FAIL  ") <> c.name)
  if Array.null failed then
    log ("all " <> show (Array.length cases) <> " cases passed")
  else
    throw (show (Array.length failed) <> " cases failed")
