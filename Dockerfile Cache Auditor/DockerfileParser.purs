module DockerfileParser
  ( Flag
  , Instruction
  , Stage
  , Dockerfile
  , parseDockerfile
  , flagValue
  , hasFlag
  , words
  , sourcesOf
  , resolveStage
  , stageDeps
  , mountFrom
  ) where

import Prelude

import Data.Array as Array
import Data.Array.NonEmpty as NEA
import Data.Foldable (foldl)
import Data.Int as Int
import Data.Maybe (Maybe(..), fromMaybe, isJust)
import Data.String as Str
import Data.String.CodeUnits as CU
import Data.String.Regex as Rx
import Data.String.Regex.Flags (global, noFlags)
import Data.String.Regex.Unsafe (unsafeRegex)
import PathGlob (normalizePath)

type Flag = { name :: String, value :: String }

type Instruction =
  { keyword :: String
  , args :: String
  , flags :: Array Flag
  , line :: Int
  , endLine :: Int
  }

type Stage =
  { index :: Int
  , name :: Maybe String
  , image :: String
  , from :: Instruction
  , body :: Array Instruction
  }

type Dockerfile =
  { escape :: Char
  , syntax :: Maybe String
  , preamble :: Array Instruction
  , stages :: Array Stage
  , stray :: Array Instruction
  }

whitespaceRe :: Rx.Regex
whitespaceRe = unsafeRegex "\\s+" noFlags

firstSpaceRe :: Rx.Regex
firstSpaceRe = unsafeRegex "\\s" noFlags

trailingSpaceRe :: Rx.Regex
trailingSpaceRe = unsafeRegex "\\s+$" noFlags

leadingTabsRe :: Rx.Regex
leadingTabsRe = unsafeRegex "^\\t+" noFlags

directiveRe :: Rx.Regex
directiveRe = unsafeRegex "^#\\s*([A-Za-z]+)\\s*=\\s*(.*?)\\s*$" noFlags

heredocAllRe :: Rx.Regex
heredocAllRe = unsafeRegex "(?<!<)<<-?\\s*[\"']?[A-Za-z_][A-Za-z0-9_]*[\"']?" global

heredocOneRe :: Rx.Regex
heredocOneRe = unsafeRegex "<<(-?)\\s*[\"']?([A-Za-z_][A-Za-z0-9_]*)" noFlags

words :: String -> Array String
words s = Array.filter (not <<< Str.null) (Rx.split whitespaceRe s)

trimEnd :: String -> String
trimEnd = Rx.replace trailingSpaceRe ""

groups :: Rx.Regex -> String -> Maybe (Array (Maybe String))
groups re s = map NEA.toArray (Rx.match re s)

isSkippable :: String -> Boolean
isSkippable l =
  let
    t = Str.trim l
  in
    t == "" || CU.take 1 t == "#"

firstToken :: String -> String
firstToken s = case Rx.search firstSpaceRe s of
  Just i -> CU.take i s
  Nothing -> s

readDirectives :: Array String -> Array { key :: String, value :: String }
readDirectives ls = Array.mapMaybe one (Array.takeWhile (\l -> isJust (Rx.match directiveRe (Str.trim l))) ls)
  where
  one l = do
    g <- groups directiveRe (Str.trim l)
    key <- join (Array.index g 1)
    value <- join (Array.index g 2)
    pure { key, value }

readLogical :: Char -> Array String -> Int -> { text :: String, next :: Int }
readLogical esc ls start = loop start ""
  where
  escS = CU.singleton esc

  loop :: Int -> String -> { text :: String, next :: Int }
  loop i buf = case Array.index ls i of
    Nothing -> { text: buf, next: i }
    Just l
      | i > start && isSkippable l -> loop (i + 1) buf
      | otherwise ->
          let
            t = trimEnd l
          in
            if CU.takeRight 1 t == escS then loop (i + 1) (buf <> CU.dropRight 1 t)
            else { text: buf <> t, next: i + 1 }

heredocDelims :: String -> Array { name :: String, strip :: Boolean }
heredocDelims text = case Rx.match heredocAllRe text of
  Nothing -> []
  Just m -> Array.mapMaybe one (map (fromMaybe "") (NEA.toArray m))
  where
  one s = do
    g <- groups heredocOneRe s
    name <- join (Array.index g 2)
    pure { name, strip: join (Array.index g 1) == Just "-" }

readHeredocs :: Array String -> Int -> String -> { text :: String, next :: Int }
readHeredocs ls start text = foldl step { text, next: start } (heredocDelims text)
  where
  step acc d = loop acc d acc.next []

  loop acc d i body = case Array.index ls i of
    Nothing -> { text: acc.text <> "\n" <> Str.joinWith "\n" body, next: i }
    Just l ->
      let
        candidate = trimEnd (if d.strip then Rx.replace leadingTabsRe "" l else l)
      in
        if candidate == d.name then { text: acc.text <> "\n" <> Str.joinWith "\n" body, next: i + 1 }
        else loop acc d (i + 1) (Array.snoc body l)

toFlag :: String -> Flag
toFlag tok =
  let
    body = CU.drop 2 tok
  in
    case CU.indexOf (Str.Pattern "=") body of
      Just i -> { name: Str.toLower (CU.take i body), value: CU.drop (i + 1) body }
      Nothing -> { name: Str.toLower body, value: "" }

extractFlags :: String -> { flags :: Array Flag, rest :: String }
extractFlags = go []
  where
  go acc str
    | CU.take 2 str == "--" =
        let
          tok = firstToken str
        in
          go (Array.snoc acc (toFlag tok)) (Str.trim (CU.drop (CU.length tok) str))
    | otherwise = { flags: acc, rest: str }

takesFlags :: String -> Boolean
takesFlags kw = Array.elem kw [ "RUN", "COPY", "ADD", "FROM" ]

takesHeredoc :: String -> Boolean
takesHeredoc kw = Array.elem kw [ "RUN", "COPY", "ADD" ]

mkInstruction :: Int -> Int -> String -> Instruction
mkInstruction startLine endLine text =
  let
    t = Str.trim text
    kw = Str.toUpper (firstToken t)
    rest = Str.trim (CU.drop (CU.length (firstToken t)) t)
    parsed = if takesFlags kw then extractFlags rest else { flags: [], rest }
  in
    { keyword: kw, args: parsed.rest, flags: parsed.flags, line: startLine, endLine }

collect :: Char -> Array String -> Array Instruction
collect esc ls = go 0 []
  where
  go :: Int -> Array Instruction -> Array Instruction
  go i acc = case Array.index ls i of
    Nothing -> acc
    Just l
      | isSkippable l -> go (i + 1) acc
      | otherwise ->
          let
            logical = readLogical esc ls i
            kw = Str.toUpper (firstToken (Str.trim logical.text))
            full = if takesHeredoc kw then readHeredocs ls logical.next logical.text else logical
          in
            go full.next (Array.snoc acc (mkInstruction (i + 1) full.next full.text))

newStage :: Int -> Instruction -> Stage
newStage idx i =
  let
    ws = words i.args
  in
    { index: idx
    , name: case Array.drop 1 ws of
        [ kw, nm ] | Str.toLower kw == "as" -> Just nm
        _ -> Nothing
    , image: fromMaybe "" (Array.head ws)
    , from: i
    , body: []
    }

parseDockerfile :: String -> Dockerfile
parseDockerfile source =
  let
    cleaned = fromMaybe source (Str.stripPrefix (Str.Pattern "\xFEFF") source)
    ls = Str.split (Str.Pattern "\n") (Str.replaceAll (Str.Pattern "\r\n") (Str.Replacement "\n") cleaned)
    directives = readDirectives ls
    esc = case Array.find (\d -> Str.toLower d.key == "escape") directives of
      Just d | d.value == "`" -> '`'
      _ -> '\\'
    syntax = map _.value (Array.find (\d -> Str.toLower d.key == "syntax") directives)
    instrs = collect esc ls
    step st i
      | i.keyword == "FROM" = st { stages = Array.snoc st.stages (newStage (Array.length st.stages) i) }
      | otherwise = case Array.unsnoc st.stages of
          Just { init, last } -> st { stages = Array.snoc init (last { body = Array.snoc last.body i }) }
          Nothing ->
            if i.keyword == "ARG" then st { preamble = Array.snoc st.preamble i }
            else st { stray = Array.snoc st.stray i }
    done = foldl step { preamble: [], stages: [], stray: [] } instrs
  in
    { escape: esc, syntax, preamble: done.preamble, stages: done.stages, stray: done.stray }

flagValue :: String -> Instruction -> Maybe String
flagValue name i = map _.value (Array.find (\f -> f.name == name) i.flags)

hasFlag :: String -> Instruction -> Boolean
hasFlag name i = isJust (flagValue name i)

sourcesOf :: Instruction -> { sources :: Array String, dest :: String }
sourcesOf i
  | Str.contains (Str.Pattern "<<") i.args = { sources: [], dest: "" }
  | otherwise =
      let
        ws =
          if CU.take 1 i.args == "[" then
            Array.filter (not <<< Str.null) (map unquote (Str.split (Str.Pattern ",") (stripBrackets i.args)))
          else words i.args
      in
        case Array.unsnoc ws of
          Just { init, last } -> { sources: map normalizePath init, dest: last }
          Nothing -> { sources: [], dest: "" }
  where
  stripBrackets s = CU.dropRight 1 (CU.drop 1 (Str.trim s))
  unquote s = Rx.replace (unsafeRegex "^[\\s\"']+|[\\s\"']+$" global) "" s

mountFrom :: Instruction -> Array String
mountFrom i = Array.mapMaybe pick (Array.filter (\f -> f.name == "mount") i.flags)
  where
  pick f = Array.findMap
    ( \kv -> case CU.indexOf (Str.Pattern "=") kv of
        Just k | CU.take k kv == "from" -> Just (CU.drop (k + 1) kv)
        _ -> Nothing
    )
    (Str.split (Str.Pattern ",") f.value)

resolveStage :: Dockerfile -> String -> Maybe Int
resolveStage df ref = case Int.fromString ref of
  Just n | n >= 0 && n < Array.length df.stages -> Just n
  Just _ -> Nothing
  Nothing -> map _.index (Array.find (\s -> map Str.toLower s.name == Just (Str.toLower ref)) df.stages)

stageDeps :: Dockerfile -> Stage -> Array Int
stageDeps df st = Array.nub (Array.filter (_ < st.index) (Array.catMaybes (Array.concat [ fromBase, fromBody ])))
  where
  fromBase = [ map _.index (Array.find (\s -> map Str.toLower s.name == Just (Str.toLower st.image)) df.stages) ]
  fromBody = Array.concatMap refs st.body
  refs i = map (resolveStage df) (Array.catMaybes [ flagValue "from" i ] <> mountFrom i)
