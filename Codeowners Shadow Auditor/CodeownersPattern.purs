module CodeownersPattern
  ( Token(..)
  , Pattern
  , compilePattern
  , matchesPath
  , splitPath
  , segmentGlobMatch
  , segmentGlobCovers
  , covers
  , sameShape
  , hasWildcard
  ) where

import Prelude

import Data.Array as Array
import Data.Foldable (all, any)
import Data.Maybe (Maybe(..), fromMaybe)
import Data.String as Str
import Data.String.CodeUnits as CU

data Token
  = DoubleStar
  | Glob String

derive instance eqToken :: Eq Token

instance showToken :: Show Token where
  show DoubleStar = "**"
  show (Glob g) = g

type Pattern =
  { source :: String
  , tokens :: Array Token
  , anchored :: Boolean
  , dirOnly :: Boolean
  , cascade :: Boolean
  }

hasWildcard :: String -> Boolean
hasWildcard g = any (\c -> c == '*' || c == '?') (CU.toCharArray g)

collapseStars :: String -> String
collapseStars g = CU.fromCharArray (Array.foldr step [] (CU.toCharArray g))
  where
  step '*' acc = case Array.head acc of
    Just '*' -> acc
    _ -> Array.cons '*' acc
  step c acc = Array.cons c acc

toToken :: String -> Token
toToken "**" = DoubleStar
toToken s = Glob (collapseStars s)

collapseDouble :: Array Token -> Array Token
collapseDouble = Array.foldl step []
  where
  step acc DoubleStar = case Array.last acc of
    Just DoubleStar -> acc
    _ -> Array.snoc acc DoubleStar
  step acc t = Array.snoc acc t

compilePattern :: String -> Maybe Pattern
compilePattern source =
  let
    leadingSlash = CU.take 1 source == "/"
    body = if leadingSlash then CU.drop 1 source else source
    trailingSlash = CU.takeRight 1 body == "/"
    rawSegs = Array.filter (_ /= "") (Str.split (Str.Pattern "/") body)
    anchored = leadingSlash || Array.length rawSegs > 1
    toks0 = collapseDouble (map toToken rawSegs)
    endsInDouble = Array.last toks0 == Just DoubleStar
    toks1 = if endsInDouble then fromMaybe [] (Array.init toks0) else toks0
    dirOnly = trailingSlash || endsInDouble
  in
    if Array.null rawSegs then
      Nothing
    else if Array.null toks1 then
      Just
        { source
        , tokens: [ DoubleStar, Glob "*" ]
        , anchored: false
        , dirOnly: false
        , cascade: false
        }
    else
      let
        tokens = if anchored then toks1 else Array.cons DoubleStar toks1
        literalTail = case Array.last toks1 of
          Just (Glob g) -> not (hasWildcard g)
          _ -> false
      in
        Just
          { source
          , tokens
          , anchored
          , dirOnly
          , cascade: dirOnly || literalTail
          }

splitPath :: String -> Array String
splitPath p =
  let
    trimmed = if CU.take 2 p == "./" then CU.drop 2 p else p
  in
    Array.filter (_ /= "") (Str.split (Str.Pattern "/") trimmed)

segmentGlobMatch :: String -> String -> Boolean
segmentGlobMatch g s = Array.elem (Array.length gs) final
  where
  gs = CU.toCharArray g

  climb :: Int -> Array Int
  climb j = case Array.index gs j of
    Just '*' -> Array.cons j (climb (j + 1))
    _ -> [ j ]

  expand :: Array Int -> Array Int
  expand xs = Array.nub (Array.concatMap climb xs)

  advance :: Char -> Int -> Array Int
  advance c j = case Array.index gs j of
    Just '*' -> [ j ]
    Just '?' -> [ j + 1 ]
    Just d | d == c -> [ j + 1 ]
    _ -> []

  step :: Array Int -> Char -> Array Int
  step xs c = expand (Array.concatMap (advance c) xs)

  final = Array.foldl step (expand [ 0 ]) (CU.toCharArray s)

segmentGlobCovers :: String -> String -> Boolean
segmentGlobCovers h g = go (CU.toCharArray h) (CU.toCharArray g)
  where
  go :: Array Char -> Array Char -> Boolean
  go hs gs = case Array.uncons hs of
    Nothing -> Array.null gs
    Just { head: '*', tail: ht } ->
      go ht gs || case Array.uncons gs of
        Nothing -> false
        Just { tail: gt } -> go hs gt
    Just { head: hc, tail: ht } -> case Array.uncons gs of
      Nothing -> false
      Just { head: gc, tail: gt }
        | gc == '*' -> false
        | hc == '?' -> go ht gt
        | hc == gc -> go ht gt
        | otherwise -> false

matchTokens :: Array Token -> Array String -> Boolean
matchTokens ts ss = case Array.uncons ts of
  Nothing -> Array.null ss
  Just { head: DoubleStar, tail: tt } ->
    matchTokens tt ss || case Array.uncons ss of
      Nothing -> false
      Just { tail: rest } -> matchTokens ts rest
  Just { head: Glob g, tail: tt } -> case Array.uncons ss of
    Nothing -> false
    Just { head: s, tail: rest } -> segmentGlobMatch g s && matchTokens tt rest

matchesPath :: Pattern -> Array String -> Boolean
matchesPath p segs = n > 0 && any ok (Array.range 1 n)
  where
  n = Array.length segs
  ok k = fits k && matchTokens p.tokens (Array.take k segs)
  fits k
    | p.dirOnly = k < n
    | p.cascade = true
    | otherwise = k == n

isUniversal :: String -> Boolean
isUniversal h = h /= "" && all (_ == '*') (CU.toCharArray h)

simulate :: Boolean -> Array Token -> Array Token -> Boolean
simulate absorb ta tb = Array.elem n final
  where
  n = Array.length tb

  climb :: Int -> Array Int
  climb j = case Array.index tb j of
    Just DoubleStar -> Array.cons j (climb (j + 1))
    _ -> [ j ]

  closure :: Array Int -> Array Int
  closure xs = Array.sort (Array.nub (Array.concatMap climb xs))

  covered :: String -> Maybe String -> Boolean
  covered h Nothing = isUniversal h
  covered h (Just g) = segmentGlobCovers h g

  consume :: Maybe String -> Int -> Array Int
  consume sym j = case Array.index tb j of
    Nothing -> if absorb then [ j ] else []
    Just DoubleStar -> [ j ]
    Just (Glob h) -> if covered h sym then [ j + 1 ] else []

  stepSym :: Maybe String -> Array Int -> Array Int
  stepSym sym xs = closure (Array.concatMap (consume sym) xs)

  orbit :: Int -> Array (Array Int) -> Array Int -> Maybe (Array (Array Int))
  orbit fuel seen cur
    | Array.elem cur seen = Just seen
    | fuel <= 0 = Nothing
    | otherwise = orbit (fuel - 1) (Array.snoc seen cur) (stepSym Nothing cur)

  stepRun :: Array Int -> Array Int
  stepRun xs = case orbit 64 [] xs of
    Nothing -> []
    Just sets -> Array.foldl Array.intersect (fromMaybe [] (Array.head sets)) sets

  stepTok :: Array Int -> Token -> Array Int
  stepTok xs DoubleStar = stepRun xs
  stepTok xs (Glob g) = stepSym (Just g) xs

  final = Array.foldl stepTok (closure [ 0 ]) ta

covers :: Pattern -> Pattern -> Boolean
covers b a =
  if b.dirOnly then all (\t -> simulate true t b.tokens) properPrefixes
  else if b.cascade then simulate true full b.tokens
  else simulate false full b.tokens
  where
  ts = a.tokens
  initTs = fromMaybe [] (Array.init ts)
  full
    | a.dirOnly = ts <> [ Glob "*", DoubleStar ]
    | a.cascade = ts <> [ DoubleStar ]
    | otherwise = ts
  properPrefixes
    | a.dirOnly = [ ts <> [ DoubleStar ] ]
    | a.cascade = [ initTs, ts <> [ DoubleStar ] ]
    | otherwise = [ initTs ]

sameShape :: Pattern -> Pattern -> Boolean
sameShape a b = a.tokens == b.tokens && a.dirOnly == b.dirOnly && a.cascade == b.cascade
