module PathGlob
  ( normalizePath
  , segments
  , contextMatches
  , dockerignoreIgnores
  , parseIgnoreLines
  ) where

import Prelude

import Data.Array as Array
import Data.Foldable (any, foldl)
import Data.Maybe (Maybe(..))
import Data.String as Str
import Data.String.CodeUnits as CU
import Data.String.Regex as Rx
import Data.String.Regex.Flags (noFlags)
import Data.String.Regex.Unsafe (unsafeRegex)

leadingRe :: Rx.Regex
leadingRe = unsafeRegex "^(\\./|/)+" noFlags

trailingRe :: Rx.Regex
trailingRe = unsafeRegex "/+$" noFlags

normalizePath :: String -> String
normalizePath p =
  let
    stripped = Rx.replace trailingRe "" (Rx.replace leadingRe "" (Str.trim p))
  in
    if stripped == "." then "" else stripped

segments :: String -> Array String
segments p = Array.filter (\s -> s /= "" && s /= ".") (Str.split (Str.Pattern "/") p)

segMatch :: Array Char -> Array Char -> Boolean
segMatch p s = case Array.uncons p of
  Nothing -> Array.null s
  Just { head: '*', tail: pt } ->
    segMatch pt s || case Array.uncons s of
      Just { tail: st } -> segMatch p st
      Nothing -> false
  Just { head: '?', tail: pt } -> case Array.uncons s of
    Just { tail: st } -> segMatch pt st
    Nothing -> false
  Just { head: c, tail: pt } -> case Array.uncons s of
    Just { head: h, tail: st } | h == c -> segMatch pt st
    _ -> false

pathMatch :: Array String -> Array String -> Boolean
pathMatch ps ss = case Array.uncons ps of
  Nothing -> Array.null ss
  Just { head: "**", tail: pt } ->
    pathMatch pt ss || case Array.uncons ss of
      Just { tail: st } -> pathMatch ps st
      Nothing -> false
  Just { head: p, tail: pt } -> case Array.uncons ss of
    Just { head: s, tail: st } -> segMatch (CU.toCharArray p) (CU.toCharArray s) && pathMatch pt st
    Nothing -> false

patternHitsPath :: String -> String -> Boolean
patternHitsPath pat path =
  let
    ps = segments (normalizePath pat)
    ss = segments path
  in
    if Array.null ps then true
    else if Array.null ss then false
    else any (\k -> pathMatch ps (Array.take k ss)) (Array.range 1 (Array.length ss))

contextMatches :: String -> String -> Boolean
contextMatches = patternHitsPath

type IgnoreRule = { pattern :: String, negate :: Boolean }

parseIgnoreLines :: Array String -> Array IgnoreRule
parseIgnoreLines ls = Array.mapMaybe one ls
  where
  one raw =
    let
      t = Str.trim raw
    in
      if t == "" || CU.take 1 t == "#" then Nothing
      else if CU.take 1 t == "!" then Just { pattern: Str.trim (CU.drop 1 t), negate: true }
      else Just { pattern: t, negate: false }

dockerignoreIgnores :: Array String -> String -> Boolean
dockerignoreIgnores ls path = foldl step false (parseIgnoreLines ls)
  where
  step acc r = if patternHitsPath r.pattern path then not r.negate else acc
