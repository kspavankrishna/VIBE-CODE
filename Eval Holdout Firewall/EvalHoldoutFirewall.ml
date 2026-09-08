type mode = Manifest | Scan

type config = {
  mode : mode;
  kgram : int;
  signature_size : int;
  threshold : float;
  window_tokens : int;
  stride_tokens : int;
  max_report : int;
  emit_clean : bool;
  manifest_path : string option;
  inputs : string list;
}

type fingerprint = {
  id : string;
  source : string;
  bytes : int;
  token_count : int;
  kgram_count : int;
  signature : int64 array;
}

type hit = {
  candidate : string;
  chunk_start : int;
  chunk_end_ : int;
  candidate_tokens : int;
  candidate_kgrams : int;
  holdout : fingerprint;
  score : float;
}

exception Usage of string
exception Data_error of string

let default_config mode =
  {
    mode;
    kgram = 5;
    signature_size = 192;
    threshold = 0.72;
    window_tokens = 720;
    stride_tokens = 240;
    max_report = 40;
    emit_clean = false;
    manifest_path = None;
    inputs = [];
  }

let usage =
  String.concat "\n"
    [
      "EvalHoldoutFirewall.ml";
      "";
      "Build or scan deterministic holdout fingerprints for LLM evaluation data.";
      "";
      "Usage:";
      "  ocamlc -O2 -o eval_holdout_firewall EvalHoldoutFirewall.ml";
      "  ./eval_holdout_firewall manifest [options] holdout1.txt holdout2.jsonl > holdouts.tsv";
      "  ./eval_holdout_firewall scan --manifest holdouts.tsv [options] candidate1.jsonl candidate2.txt";
      "";
      "Options:";
      "  --k INT              normalized token k-gram length, default 5";
      "  --signature INT      number of smallest k-gram hashes retained, default 192";
      "  --threshold FLOAT    containment score that blocks a candidate, default 0.72";
      "  --window INT         scan window size in tokens, default 720";
      "  --stride INT         scan window stride in tokens, default 240";
      "  --max-report INT     maximum JSON findings printed per scan file, default 40";
      "  --emit-clean         print a clean JSON line when no threshold hit is found";
      "  --manifest PATH      manifest TSV path for scan mode";
      "  --help               show this help";
      "";
      "Input path '-' means stdin. Manifest rows are TSV and intentionally boring:";
      "id, bytes, tokens, kgrams, comma-separated 64-bit hex signature, escaped source.";
    ]

let fail_usage msg = raise (Usage msg)
let fail_data msg = raise (Data_error msg)

let parse_int name raw =
  try int_of_string raw
  with Failure _ -> fail_usage (name ^ " must be an integer, got " ^ raw)

let parse_float name raw =
  try float_of_string raw
  with Failure _ -> fail_usage (name ^ " must be a number, got " ^ raw)

let require_value name = function
  | value :: rest -> (value, rest)
  | [] -> fail_usage (name ^ " needs a value")

let rec parse_options cfg = function
  | [] -> cfg
  | "--help" :: _ -> print_endline usage; exit 0
  | "--k" :: rest ->
      let value, tail = require_value "--k" rest in
      parse_options { cfg with kgram = parse_int "--k" value } tail
  | "--signature" :: rest ->
      let value, tail = require_value "--signature" rest in
      parse_options { cfg with signature_size = parse_int "--signature" value } tail
  | "--threshold" :: rest ->
      let value, tail = require_value "--threshold" rest in
      parse_options { cfg with threshold = parse_float "--threshold" value } tail
  | "--window" :: rest ->
      let value, tail = require_value "--window" rest in
      parse_options { cfg with window_tokens = parse_int "--window" value } tail
  | "--stride" :: rest ->
      let value, tail = require_value "--stride" rest in
      parse_options { cfg with stride_tokens = parse_int "--stride" value } tail
  | "--max-report" :: rest ->
      let value, tail = require_value "--max-report" rest in
      parse_options { cfg with max_report = parse_int "--max-report" value } tail
  | "--emit-clean" :: tail -> parse_options { cfg with emit_clean = true } tail
  | "--manifest" :: rest ->
      let value, tail = require_value "--manifest" rest in
      parse_options { cfg with manifest_path = Some value } tail
  | raw :: _ when String.length raw > 0 && raw.[0] = '-' ->
      fail_usage ("unknown option " ^ raw)
  | path :: tail -> parse_options { cfg with inputs = cfg.inputs @ [ path ] } tail

let validate_config cfg =
  if cfg.kgram < 1 then fail_usage "--k must be at least 1";
  if cfg.signature_size < 16 then fail_usage "--signature must be at least 16";
  if cfg.threshold < 0.0 || cfg.threshold > 1.0 then
    fail_usage "--threshold must be between 0 and 1";
  if cfg.window_tokens < cfg.kgram then fail_usage "--window must be >= --k";
  if cfg.stride_tokens < 1 then fail_usage "--stride must be at least 1";
  if cfg.max_report < 1 then fail_usage "--max-report must be at least 1";
  match cfg.mode, cfg.manifest_path with
  | Scan, None -> fail_usage "scan mode requires --manifest PATH"
  | Manifest, Some _ -> fail_usage "manifest mode does not use --manifest"
  | _ -> cfg

let parse_args () =
  match Array.to_list Sys.argv with
  | _ :: "manifest" :: rest -> validate_config (parse_options (default_config Manifest) rest)
  | _ :: "scan" :: rest -> validate_config (parse_options (default_config Scan) rest)
  | _ :: ("--help" | "-h") :: _ -> print_endline usage; exit 0
  | _ -> fail_usage "first argument must be 'manifest' or 'scan'"

let with_open_in path f =
  let ic = open_in_bin path in
  try
    let result = f ic in
    close_in ic;
    result
  with exn ->
    close_in_noerr ic;
    raise exn

let read_all path =
  let read_channel ic =
    let buffer = Buffer.create 65536 in
    let chunk = Bytes.create 65536 in
    let rec loop () =
      match input ic chunk 0 (Bytes.length chunk) with
      | 0 -> Buffer.contents buffer
      | n ->
          Buffer.add_string buffer (Bytes.sub_string chunk 0 n);
          loop ()
    in
    loop ()
  in
  if path = "-" then read_channel stdin else with_open_in path read_channel

let lines_of_string text =
  let raw = String.split_on_char '\n' text in
  List.map
    (fun line ->
      let len = String.length line in
      if len > 0 && line.[len - 1] = '\r' then String.sub line 0 (len - 1) else line)
    raw

let lower_ascii c =
  if c >= 'A' && c <= 'Z' then Char.chr (Char.code c + 32) else c

let is_token_char = function
  | 'a' .. 'z' | 'A' .. 'Z' | '0' .. '9' | '_' -> true
  | _ -> false

let tokenize text =
  let tokens = ref [] in
  let current = Buffer.create 32 in
  let flush () =
    if Buffer.length current > 0 then (
      tokens := Buffer.contents current :: !tokens;
      Buffer.clear current)
  in
  String.iter
    (fun c ->
      if is_token_char c then Buffer.add_char current (lower_ascii c) else flush ())
    text;
  flush ();
  Array.of_list (List.rev !tokens)

let hex_digit = function
  | '0' .. '9' as c -> Char.code c - Char.code '0'
  | 'a' .. 'f' as c -> 10 + Char.code c - Char.code 'a'
  | 'A' .. 'F' as c -> 10 + Char.code c - Char.code 'A'
  | c -> fail_data ("invalid hex digit: " ^ String.make 1 c)

let int64_of_hex raw =
  let start = if String.length raw >= 2 && raw.[0] = '0' && (raw.[1] = 'x' || raw.[1] = 'X') then 2 else 0 in
  let acc = ref 0L in
  for i = start to String.length raw - 1 do
    acc := Int64.logor (Int64.shift_left !acc 4) (Int64.of_int (hex_digit raw.[i]))
  done;
  !acc

let hex_of_int64 value = Printf.sprintf "%016Lx" value

let fnv_offset = int64_of_hex "cbf29ce484222325"
let fnv_prime = 0x100000001b3L
let separator_mix = int64_of_hex "9e3779b185ebca87"

let mix64 state value = Int64.mul (Int64.logxor state value) fnv_prime

let hash_string text =
  let state = ref fnv_offset in
  for i = 0 to String.length text - 1 do
    state := mix64 !state (Int64.of_int (Char.code text.[i]))
  done;
  !state

let hash_kgram token_hashes start len =
  let state = ref fnv_offset in
  for i = start to start + len - 1 do
    state := mix64 !state token_hashes.(i);
    state := mix64 !state separator_mix
  done;
  !state

let take limit values =
  let rec loop remaining acc = function
    | _ when remaining = 0 -> List.rev acc
    | [] -> List.rev acc
    | x :: xs -> loop (remaining - 1) (x :: acc) xs
  in
  loop limit [] values

let insert_smallest limit value values =
  if List.exists (fun existing -> existing = value) values then values
  else
    let rec insert prefix = function
      | [] -> List.rev (value :: prefix)
      | x :: xs as rest ->
          if Int64.compare value x < 0 then List.rev_append prefix (value :: rest)
          else insert (x :: prefix) xs
    in
    take limit (insert [] values)

let signature_of_tokens ~kgram ~signature_size tokens =
  let token_count = Array.length tokens in
  if token_count = 0 then ([||], 0)
  else
    let token_hashes = Array.map hash_string tokens in
    let gram_len = min kgram token_count in
    let gram_count = if token_count < kgram then 1 else token_count - kgram + 1 in
    let signature = ref [] in
    for start = 0 to gram_count - 1 do
      signature := insert_smallest signature_size (hash_kgram token_hashes start gram_len) !signature
    done;
    (Array.of_list !signature, gram_count)

let array_slice source start stop =
  if stop < start then [||]
  else Array.init (stop - start) (fun i -> source.(start + i))

let safe_basename path =
  let base = Filename.basename path in
  let buffer = Buffer.create (String.length base) in
  String.iter
    (fun c ->
      match c with
      | 'a' .. 'z' | 'A' .. 'Z' | '0' .. '9' | '_' -> Buffer.add_char buffer c
      | _ -> Buffer.add_char buffer '_')
    base;
  let value = Buffer.contents buffer in
  if value = "" || value = "_" then "stdin" else value

let fingerprint_of_tokens cfg ~id ~source ~bytes tokens =
  let signature, kgram_count =
    signature_of_tokens ~kgram:cfg.kgram ~signature_size:cfg.signature_size tokens
  in
  { id; source; bytes; token_count = Array.length tokens; kgram_count; signature }

let fingerprint_of_text cfg ~source text =
  let tokens = tokenize text in
  let signature, kgram_count =
    signature_of_tokens ~kgram:cfg.kgram ~signature_size:cfg.signature_size tokens
  in
  let suffix =
    if Array.length signature = 0 then "empty" else String.sub (hex_of_int64 signature.(0)) 0 12
  in
  {
    id = safe_basename source ^ ":" ^ suffix;
    source;
    bytes = String.length text;
    token_count = Array.length tokens;
    kgram_count;
    signature;
  }

let escape_tsv text =
  let buffer = Buffer.create (String.length text) in
  String.iter
    (function
      | '\\' -> Buffer.add_string buffer "\\\\"
      | '\t' -> Buffer.add_string buffer "\\t"
      | '\n' -> Buffer.add_string buffer "\\n"
      | '\r' -> Buffer.add_string buffer "\\r"
      | c -> Buffer.add_char buffer c)
    text;
  Buffer.contents buffer

let unescape_tsv text =
  let buffer = Buffer.create (String.length text) in
  let rec loop i =
    if i >= String.length text then Buffer.contents buffer
    else if text.[i] = '\\' && i + 1 < String.length text then (
      (match text.[i + 1] with
      | '\\' -> Buffer.add_char buffer '\\'
      | 't' -> Buffer.add_char buffer '\t'
      | 'n' -> Buffer.add_char buffer '\n'
      | 'r' -> Buffer.add_char buffer '\r'
      | c -> Buffer.add_char buffer c);
      loop (i + 2))
    else (
      Buffer.add_char buffer text.[i];
      loop (i + 1))
  in
  loop 0

let signature_to_text signature =
  signature |> Array.to_list |> List.map hex_of_int64 |> String.concat ","

let signature_of_text_field raw =
  if raw = "" then [||]
  else raw |> String.split_on_char ',' |> List.map int64_of_hex |> Array.of_list

let manifest_line fp =
  String.concat "\t"
    [
      escape_tsv fp.id;
      string_of_int fp.bytes;
      string_of_int fp.token_count;
      string_of_int fp.kgram_count;
      signature_to_text fp.signature;
      escape_tsv fp.source;
    ]

let parse_manifest_line line_number line =
  match String.split_on_char '\t' line with
  | [ id; bytes; tokens; kgrams; signature; source ] ->
      let parse_int_field name raw =
        try int_of_string raw
        with Failure _ ->
          fail_data
            (Printf.sprintf "manifest line %d has invalid %s value %S" line_number name raw)
      in
      {
        id = unescape_tsv id;
        bytes = parse_int_field "bytes" bytes;
        token_count = parse_int_field "tokens" tokens;
        kgram_count = parse_int_field "kgrams" kgrams;
        signature = signature_of_text_field signature;
        source = unescape_tsv source;
      }
  | _ -> fail_data (Printf.sprintf "manifest line %d is not a six-field TSV row" line_number)

let load_manifest path =
  read_all path |> lines_of_string
  |> List.mapi (fun i line -> (i + 1, line))
  |> List.filter (fun (_, line) -> String.trim line <> "" && not (String.length line > 0 && line.[0] = '#'))
  |> List.map (fun (line_number, line) -> parse_manifest_line line_number line)

let intersection_count left right =
  let i = ref 0 in
  let j = ref 0 in
  let matches = ref 0 in
  while !i < Array.length left && !j < Array.length right do
    let cmp = Int64.compare left.(!i) right.(!j) in
    if cmp = 0 then (
      incr matches;
      incr i;
      incr j)
    else if cmp < 0 then incr i
    else incr j
  done;
  !matches

let containment_score left right =
  let denominator = min (Array.length left) (Array.length right) in
  if denominator = 0 then 0.0
  else float_of_int (intersection_count left right) /. float_of_int denominator

let json_escape text =
  let buffer = Buffer.create (String.length text + 8) in
  String.iter
    (function
      | '"' -> Buffer.add_string buffer "\\\""
      | '\\' -> Buffer.add_string buffer "\\\\"
      | '\b' -> Buffer.add_string buffer "\\b"
      | '\012' -> Buffer.add_string buffer "\\f"
      | '\n' -> Buffer.add_string buffer "\\n"
      | '\r' -> Buffer.add_string buffer "\\r"
      | '\t' -> Buffer.add_string buffer "\\t"
      | c when Char.code c < 32 -> Buffer.add_string buffer (Printf.sprintf "\\u%04x" (Char.code c))
      | c -> Buffer.add_char buffer c)
    text;
  Buffer.contents buffer

let print_hit cfg hit =
  Printf.printf
    "{\"verdict\":\"block\",\"candidate\":\"%s\",\"chunk_start_token\":%d,\"chunk_end_token\":%d,\"candidate_tokens\":%d,\"candidate_kgrams\":%d,\"holdout_id\":\"%s\",\"holdout_source\":\"%s\",\"holdout_tokens\":%d,\"holdout_kgrams\":%d,\"score\":%.6f,\"threshold\":%.6f}\n"
    (json_escape hit.candidate) hit.chunk_start hit.chunk_end_ hit.candidate_tokens
    hit.candidate_kgrams (json_escape hit.holdout.id) (json_escape hit.holdout.source)
    hit.holdout.token_count hit.holdout.kgram_count hit.score cfg.threshold

let print_clean candidate token_count chunk_count best_score =
  Printf.printf
    "{\"verdict\":\"clean\",\"candidate\":\"%s\",\"tokens\":%d,\"chunks\":%d,\"best_score\":%.6f}\n"
    (json_escape candidate) token_count chunk_count best_score

let sort_hits hits =
  List.sort
    (fun a b ->
      let by_score = compare b.score a.score in
      if by_score <> 0 then by_score
      else
        let by_candidate = compare a.candidate b.candidate in
        if by_candidate <> 0 then by_candidate else compare a.chunk_start b.chunk_start)
    hits

let scan_chunk cfg holdouts candidate tokens start stop =
  let chunk_tokens = array_slice tokens start stop in
  let chunk_fp =
    fingerprint_of_tokens cfg ~id:(candidate ^ ":" ^ string_of_int start) ~source:candidate
      ~bytes:0 chunk_tokens
  in
  let best_score = ref 0.0 in
  let hits =
    List.fold_left
      (fun acc holdout ->
        let score = containment_score chunk_fp.signature holdout.signature in
        if score > !best_score then best_score := score;
        if score >= cfg.threshold then
          {
            candidate;
            chunk_start = start;
            chunk_end_ = stop;
            candidate_tokens = chunk_fp.token_count;
            candidate_kgrams = chunk_fp.kgram_count;
            holdout;
            score;
          }
          :: acc
        else acc)
      [] holdouts
  in
  (!best_score, hits)

let scan_candidate cfg holdouts path =
  let text = read_all path in
  let tokens = tokenize text in
  let token_count = Array.length tokens in
  let rec loop start chunk_count best_score hits =
    if token_count = 0 || start >= token_count then (chunk_count, best_score, hits)
    else
      let stop = min token_count (start + cfg.window_tokens) in
      let chunk_best, chunk_hits = scan_chunk cfg holdouts path tokens start stop in
      let next_best = max best_score chunk_best in
      let next_hits = List.rev_append chunk_hits hits in
      if stop = token_count then (chunk_count + 1, next_best, next_hits)
      else loop (start + cfg.stride_tokens) (chunk_count + 1) next_best next_hits
  in
  let chunk_count, best_score, hits = loop 0 0 0.0 [] in
  let sorted = sort_hits hits |> take cfg.max_report in
  List.iter (print_hit cfg) sorted;
  if sorted = [] && cfg.emit_clean then print_clean path token_count chunk_count best_score;
  sorted <> []

let run_manifest cfg =
  let inputs = if cfg.inputs = [] then [ "-" ] else cfg.inputs in
  List.iter
    (fun path ->
      let text = read_all path in
      let fp = fingerprint_of_text cfg ~source:path text in
      print_endline (manifest_line fp))
    inputs

let run_scan cfg =
  let manifest_path = match cfg.manifest_path with Some path -> path | None -> assert false in
  let holdouts = load_manifest manifest_path in
  if holdouts = [] then fail_data ("manifest is empty: " ^ manifest_path);
  let inputs = if cfg.inputs = [] then [ "-" ] else cfg.inputs in
  let blocked = List.fold_left (fun acc path -> scan_candidate cfg holdouts path || acc) false inputs in
  if blocked then 2 else 0

let () =
  try
    let cfg = parse_args () in
    match cfg.mode with
    | Manifest -> run_manifest cfg
    | Scan -> exit (run_scan cfg)
  with
  | Usage msg ->
      prerr_endline ("usage error: " ^ msg);
      prerr_endline "";
      prerr_endline usage;
      exit 64
  | Data_error msg ->
      prerr_endline ("data error: " ^ msg);
      exit 65
  | Sys_error msg ->
      prerr_endline ("system error: " ^ msg);
      exit 74

(*
This solves the April 2026 problem where teams ship LLM evals, fine tuning sets, RAG corpora, benchmark prompts, and agent traces without knowing that protected holdout material has quietly leaked into the candidate data. Built because a normal grep, checksum, or exact file diff misses the practical failure: the copied passage has been lowercased, wrapped in JSONL, truncated by a context builder, or mixed into a tool transcript. Use it when you need a dependency-free OCaml command line guard for AI evaluation leakage detection, dataset contamination scanning, benchmark holdout protection, research reproducibility checks, CI policy gates, and GitHub Actions evidence before a model release. The trick: normalize text into stable tokens, hash overlapping k-grams, keep a bounded deterministic MinHash-style signature, then score candidate windows against every holdout with containment instead of fragile exact equality. Drop this into an eval repository, data pipeline, model registry preflight, or DevOps release gate so a human reviewer gets small JSON findings with the candidate chunk, holdout source, similarity score, and threshold before polluted data becomes a published metric.
*)
