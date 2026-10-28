module CodeownersFile
  ( Issue(..)
  , Rule
  , parseCodeowners
  , isEffective
  , validOwner
  ) where

import Prelude

import CodeownersPattern (Pattern, compilePattern)
import Data.Array as Array
import Data.Foldable (all)
import Data.Maybe (Maybe(..), isJust)
import Data.String as Str
import Data.String.CodeUnits as CU

data Issue
  = BadOwner String
  | Negation
  | CharRange
  | Escape
  | EmptyPattern
  | RepeatedOwner String

derive instance eqIssue :: Eq Issue

type Rule =
  { line :: Int
  , text :: String
  , pattern :: Maybe Pattern
  , owners :: Array String
  , issues :: Array Issue
  }

isEffective :: Rule -> Boolean
isEffective r = isJust r.pattern && all benign r.issues
  where
  benign (RepeatedOwner _) = true
  benign _ = false

isAlnum :: Char -> Boolean
isAlnum c = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')

validName :: String -> Boolean
validName n = n /= "" && Str.length n <= 100 && all ok (CU.toCharArray n)
  where
  ok c = isAlnum c || c == '-' || c == '_' || c == '.'

validEmail :: String -> Boolean
validEmail o = case Str.split (Str.Pattern "@") o of
  [ local, domain ] ->
    local /= ""
      && Str.contains (Str.Pattern ".") domain
      && CU.take 1 domain /= "."
      && CU.takeRight 1 domain /= "."
  _ -> false

validOwner :: String -> Boolean
validOwner o
  | CU.take 1 o == "@" = case Str.split (Str.Pattern "/") (CU.drop 1 o) of
      [ u ] -> validName u
      [ org, team ] -> validName org && validName team
      _ -> false
  | otherwise = validEmail o

words :: String -> Array String
words s =
  Array.filter (_ /= "")
    (Str.split (Str.Pattern " ") (Str.replaceAll (Str.Pattern "\t") (Str.Replacement " ") s))

repeated :: Array String -> Array String
repeated os = Array.nub (Array.filter dup os)
  where
  dup o = Array.length (Array.filter (_ == o) os) > 1

parseLine :: Int -> String -> Maybe Rule
parseLine n rawLine =
  if t == "" || CU.take 1 t == "#" then Nothing
  else case Array.uncons (words t) of
      Nothing -> Nothing
      Just { head: first, tail } ->
        let
          escapedHash = CU.take 2 first == "\\#"
          patText = if escapedHash then CU.drop 1 first else first
          owners = Array.takeWhile (\w -> CU.take 1 w /= "#") tail
          pattern = compilePattern patText
          inspected = if escapedHash then CU.drop 2 first else first
          issues = Array.concat
            [ if CU.take 1 patText == "!" then [ Negation ] else []
            , if Str.contains (Str.Pattern "[") patText || Str.contains (Str.Pattern "]") patText then [ CharRange ] else []
            , if Str.contains (Str.Pattern "\\") inspected then [ Escape ] else []
            , if isJust pattern then [] else [ EmptyPattern ]
            , map BadOwner (Array.filter (not <<< validOwner) owners)
            , map RepeatedOwner (repeated owners)
            ]
        in
          Just { line: n, text: patText, pattern, owners, issues }
  where
  t = Str.trim rawLine

parseCodeowners :: String -> Array Rule
parseCodeowners content =
  Array.catMaybes (Array.mapWithIndex (\i l -> parseLine (i + 1) l) ls)
  where
  ls = Str.split (Str.Pattern "\n") (Str.replaceAll (Str.Pattern "\r") (Str.Replacement "") content)
