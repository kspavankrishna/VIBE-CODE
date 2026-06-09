(*
  EvalArtifactLineageGate.ml

  A dependency-free OCaml command line utility for making AI evaluation evidence
  auditable in CI. It reads a simple key=value ledger, canonicalizes records,
  computes SHA-256 record hashes, builds an append-only chain hash, and gates a
  candidate run against a baseline without storing raw prompts or private data.
*)

exception User_error of string

let user_error message = raise (User_error message)

let is_space = function
  | ' ' | '\t' | '\n' | '\r' -> true
  | _ -> false

let trim value =
  let length = String.length value in
  let left = ref 0 in
  let right = ref (length - 1) in
  while !left < length && is_space value.[!left] do
    incr left
  done;
  while !right >= !left && is_space value.[!right] do
    decr right
  done;
  if !right < !left then "" else String.sub value !left (!right - !left + 1)

let split_words value =
  let length = String.length value in
  let rec skip_spaces index =
    if index >= length then index
    else if is_space value.[index] then skip_spaces (index + 1)
    else index
  in
  let rec take_word index =
    if index >= length || is_space value.[index] then index else take_word (index + 1)
  in
  let rec loop index acc =
    let start = skip_spaces index in
    if start >= length then List.rev acc
    else
      let stop = take_word start in
      let token = String.sub value start (stop - start) in
      loop stop (token :: acc)
  in
  loop 0 []

let split_csv value =
  value
  |> String.split_on_char ','
  |> List.map trim
  |> List.filter (fun item -> item <> "")

let hex_value = function
  | '0' .. '9' as c -> Char.code c - Char.code '0'
  | 'a' .. 'f' as c -> 10 + Char.code c - Char.code 'a'
  | 'A' .. 'F' as c -> 10 + Char.code c - Char.code 'A'
  | c -> user_error (Printf.sprintf "invalid hex digit in percent escape: %c" c)

let percent_decode value =
  let length = String.length value in
  let buffer = Buffer.create length in
  let rec loop index =
    if index >= length then Buffer.contents buffer
    else
      match value.[index] with
      | '%' when index + 2 < length ->
          let hi = hex_value value.[index + 1] in
          let lo = hex_value value.[index + 2] in
          Buffer.add_char buffer (Char.chr ((hi lsl 4) lor lo));
          loop (index + 3)
      | '%' -> user_error "truncated percent escape in ledger value"
      | c ->
          Buffer.add_char buffer c;
          loop (index + 1)
  in
  loop 0

let is_canonical_safe = function
  | 'a' .. 'z' | 'A' .. 'Z' | '0' .. '9' | '-' | '_' | '.' | '/' | ':' -> true
  | _ -> false

let percent_encode value =
  let buffer = Buffer.create (String.length value) in
  String.iter
    (fun c ->
      if is_canonical_safe c then Buffer.add_char buffer c
      else Printf.bprintf buffer "%%%02X" (Char.code c))
    value;
  Buffer.contents buffer

module StringMap = Map.Make (String)
module StringSet = Set.Make (String)

let set_of_list items = List.fold_left (fun acc item -> StringSet.add item acc) StringSet.empty items

let find_opt key map =
  try Some (StringMap.find key map) with Not_found -> None

let default_ignored_keys =
  set_of_list
    [
      "timestamp";
      "time";
      "observed_at";
      "created_at";
      "updated_at";
      "host";
      "hostname";
      "pid";
      "run_id";
      "trace_id";
      "span_id";
      "request_id";
      "nonce";
      "signature";
      "record_hash";
      "prev_chain_hash";
      "previous_chain_hash";
      "chain_hash";
    ]

let valid_key key =
  let length = String.length key in
  length > 0
  && Seq.for_all
       (function
         | 'a' .. 'z' | '0' .. '9' | '_' | '-' | '.' -> true
         | _ -> false)
       (String.to_seq key)

let normalize_key key = String.lowercase_ascii (trim key)

module Sha256 = struct
  let k =
    [|
      0x428a2f98l; 0x71374491l; 0xb5c0fbcfl; 0xe9b5dba5l; 0x3956c25bl; 0x59f111f1l;
      0x923f82a4l; 0xab1c5ed5l; 0xd807aa98l; 0x12835b01l; 0x243185bel; 0x550c7dc3l;
      0x72be5d74l; 0x80deb1fel; 0x9bdc06a7l; 0xc19bf174l; 0xe49b69c1l; 0xefbe4786l;
      0x0fc19dc6l; 0x240ca1ccl; 0x2de92c6fl; 0x4a7484aal; 0x5cb0a9dcl; 0x76f988dal;
      0x983e5152l; 0xa831c66dl; 0xb00327c8l; 0xbf597fc7l; 0xc6e00bf3l; 0xd5a79147l;
      0x06ca6351l; 0x14292967l; 0x27b70a85l; 0x2e1b2138l; 0x4d2c6dfcl; 0x53380d13l;
      0x650a7354l; 0x766a0abbl; 0x81c2c92el; 0x92722c85l; 0xa2bfe8a1l; 0xa81a664bl;
      0xc24b8b70l; 0xc76c51a3l; 0xd192e819l; 0xd6990624l; 0xf40e3585l; 0x106aa070l;
      0x19a4c116l; 0x1e376c08l; 0x2748774cl; 0x34b0bcb5l; 0x391c0cb3l; 0x4ed8aa4al;
      0x5b9cca4fl; 0x682e6ff3l; 0x748f82eel; 0x78a5636fl; 0x84c87814l; 0x8cc70208l;
      0x90befffal; 0xa4506cebl; 0xbef9a3f7l; 0xc67178f2l;
    |]

  let rotr value bits =
    Int32.logor
      (Int32.shift_right_logical value bits)
      (Int32.shift_left value (32 - bits))

  let ch x y z = Int32.logxor (Int32.logand x y) (Int32.logand (Int32.lognot x) z)

  let maj x y z =
    Int32.logxor (Int32.logxor (Int32.logand x y) (Int32.logand x z)) (Int32.logand y z)

  let add4 a b c d = Int32.add (Int32.add a b) (Int32.add c d)
  let add5 a b c d e = Int32.add (add4 a b c d) e

  let small_sigma0 x =
    Int32.logxor (Int32.logxor (rotr x 7) (rotr x 18)) (Int32.shift_right_logical x 3)

  let small_sigma1 x =
    Int32.logxor (Int32.logxor (rotr x 17) (rotr x 19)) (Int32.shift_right_logical x 10)

  let big_sigma0 x = Int32.logxor (Int32.logxor (rotr x 2) (rotr x 13)) (rotr x 22)
  let big_sigma1 x = Int32.logxor (Int32.logxor (rotr x 6) (rotr x 11)) (rotr x 25)

  let load_be32 bytes offset =
    let byte index = Int32.of_int (Char.code bytes.[offset + index]) in
    Int32.logor
      (Int32.shift_left (byte 0) 24)
      (Int32.logor
         (Int32.shift_left (byte 1) 16)
         (Int32.logor (Int32.shift_left (byte 2) 8) (byte 3)))

  let store_be32 output offset word =
    for index = 0 to 3 do
      let shift = 8 * (3 - index) in
      let byte = Int32.to_int (Int32.logand (Int32.shift_right_logical word shift) 0xffl) in
      Bytes.set output (offset + index) (Char.chr byte)
    done

  let pad message =
    let length = String.length message in
    let bit_length = Int64.mul (Int64.of_int length) 8L in
    let remainder = (length + 1 + 8) mod 64 in
    let zeroes = if remainder = 0 then 0 else 64 - remainder in
    let total = length + 1 + zeroes + 8 in
    let padded = Bytes.make total '\000' in
    Bytes.blit_string message 0 padded 0 length;
    Bytes.set padded length (Char.chr 0x80);
    for index = 0 to 7 do
      let shift = 8 * (7 - index) in
      let byte = Int64.to_int (Int64.logand (Int64.shift_right_logical bit_length shift) 0xffL) in
      Bytes.set padded (total - 8 + index) (Char.chr byte)
    done;
    Bytes.unsafe_to_string padded

  let digest_bytes message =
    let padded = pad message in
    let h0 = ref 0x6a09e667l in
    let h1 = ref 0xbb67ae85l in
    let h2 = ref 0x3c6ef372l in
    let h3 = ref 0xa54ff53al in
    let h4 = ref 0x510e527fl in
    let h5 = ref 0x9b05688cl in
    let h6 = ref 0x1f83d9abl in
    let h7 = ref 0x5be0cd19l in
    let blocks = String.length padded / 64 in
    for block = 0 to blocks - 1 do
      let base = block * 64 in
      let w = Array.make 64 0l in
      for index = 0 to 15 do
        w.(index) <- load_be32 padded (base + (index * 4))
      done;
      for index = 16 to 63 do
        w.(index) <- add4 w.(index - 16) (small_sigma0 w.(index - 15)) w.(index - 7) (small_sigma1 w.(index - 2))
      done;
      let a = ref !h0 in
      let b = ref !h1 in
      let c = ref !h2 in
      let d = ref !h3 in
      let e = ref !h4 in
      let f = ref !h5 in
      let g = ref !h6 in
      let h = ref !h7 in
      for index = 0 to 63 do
        let t1 = add5 !h (big_sigma1 !e) (ch !e !f !g) k.(index) w.(index) in
        let t2 = Int32.add (big_sigma0 !a) (maj !a !b !c) in
        h := !g;
        g := !f;
        f := !e;
        e := Int32.add !d t1;
        d := !c;
        c := !b;
        b := !a;
        a := Int32.add t1 t2
      done;
      h0 := Int32.add !h0 !a;
      h1 := Int32.add !h1 !b;
      h2 := Int32.add !h2 !c;
      h3 := Int32.add !h3 !d;
      h4 := Int32.add !h4 !e;
      h5 := Int32.add !h5 !f;
      h6 := Int32.add !h6 !g;
      h7 := Int32.add !h7 !h
    done;
    let output = Bytes.make 32 '\000' in
    List.iteri
      (fun index word -> store_be32 output (index * 4) !word)
      [ h0; h1; h2; h3; h4; h5; h6; h7 ];
    Bytes.unsafe_to_string output

  let hex message =
    let bytes = digest_bytes message in
    let buffer = Buffer.create 64 in
    String.iter (fun c -> Printf.bprintf buffer "%02x" (Char.code c)) bytes;
    Buffer.contents buffer
end

type evidence_record = {
  line_no : int;
  fields : string StringMap.t;
  canonical : string;
  record_hash : string;
  previous_chain_hash : string;
  chain_hash : string;
}

let parse_field ~line_no token fields =
  match String.index_opt token '=' with
  | None -> user_error (Printf.sprintf "line %d: expected key=value field, got %S" line_no token)
  | Some index ->
      let key = normalize_key (String.sub token 0 index) in
      if not (valid_key key) then
        user_error (Printf.sprintf "line %d: invalid field key %S" line_no key);
      if StringMap.mem key fields then
        user_error (Printf.sprintf "line %d: duplicate field key %S" line_no key);
      let raw_value = String.sub token (index + 1) (String.length token - index - 1) in
      StringMap.add key (percent_decode raw_value) fields

let split_line_fields line =
  if String.contains line '\t' then
    line
    |> String.split_on_char '\t'
    |> List.map trim
    |> List.filter (fun token -> token <> "")
  else split_words line

let canonicalize ignore_keys fields =
  let pairs =
    StringMap.bindings fields
    |> List.filter (fun (key, _) -> not (StringSet.mem key ignore_keys))
    |> List.map (fun (key, value) -> key ^ "=" ^ percent_encode value)
  in
  match pairs with
  | [] -> user_error "record has no canonical fields after ignored keys are removed"
  | _ -> String.concat "\n" pairs

let expected_hash_value key fields =
  match find_opt key fields with
  | None -> None
  | Some value -> Some (String.lowercase_ascii (trim value))

let parse_ledger_line ~ignore_keys ~previous_chain_hash line_no raw_line =
  let line = trim raw_line in
  if line = "" || line.[0] = '#' then None
  else
    let fields =
      split_line_fields line
      |> List.fold_left (fun acc token -> parse_field ~line_no token acc) StringMap.empty
    in
    let canonical = canonicalize ignore_keys fields in
    let record_hash = Sha256.hex canonical in
    let chain_hash = Sha256.hex (previous_chain_hash ^ "\n" ^ record_hash ^ "\n" ^ canonical) in
    (match expected_hash_value "prev_chain_hash" fields with
    | Some expected when expected <> previous_chain_hash ->
        user_error
          (Printf.sprintf "line %d: prev_chain_hash mismatch, expected %s but computed previous hash is %s" line_no expected previous_chain_hash)
    | _ -> ());
    (match expected_hash_value "previous_chain_hash" fields with
    | Some expected when expected <> previous_chain_hash ->
        user_error
          (Printf.sprintf "line %d: previous_chain_hash mismatch, expected %s but computed previous hash is %s" line_no expected previous_chain_hash)
    | _ -> ());
    (match expected_hash_value "record_hash" fields with
    | Some expected when expected <> record_hash ->
        user_error
          (Printf.sprintf "line %d: record_hash mismatch, expected %s but computed %s" line_no expected record_hash)
    | _ -> ());
    (match expected_hash_value "chain_hash" fields with
    | Some expected when expected <> chain_hash ->
        user_error
          (Printf.sprintf "line %d: chain_hash mismatch, expected %s but computed %s" line_no expected chain_hash)
    | _ -> ());
    Some { line_no; fields; canonical; record_hash; previous_chain_hash; chain_hash }

let read_lines path =
  let channel = if path = "-" then stdin else open_in_bin path in
  let close_channel () = if path <> "-" then close_in_noerr channel in
  let rec loop acc =
    try
      let line = input_line channel in
      loop (line :: acc)
    with End_of_file ->
      close_channel ();
      List.rev acc
  in
  try loop [] with exn -> close_channel (); raise exn

let read_ledger ~ignore_keys path =
  let previous = ref (String.make 64 '0') in
  let records = ref [] in
  read_lines path
  |> List.iteri (fun index line ->
         let line_no = index + 1 in
         match parse_ledger_line ~ignore_keys ~previous_chain_hash:!previous line_no line with
         | None -> ()
         | Some record ->
             previous := record.chain_hash;
             records := record :: !records);
  List.rev !records

let parse_int_value ~line_no ~key value =
  try int_of_string value
  with Failure _ -> user_error (Printf.sprintf "line %d: field %s must be an integer, got %S" line_no key value)

let parse_float_value ~line_no ~key value =
  try float_of_string value
  with Failure _ -> user_error (Printf.sprintf "line %d: field %s must be a number, got %S" line_no key value)

let rec first_present fields = function
  | [] -> None
  | key :: rest -> (match find_opt key fields with Some value -> Some (key, value) | None -> first_present fields rest)

let int_field record keys =
  match first_present record.fields keys with
  | None -> 0
  | Some (key, value) -> parse_int_value ~line_no:record.line_no ~key value

let float_field record keys =
  match first_present record.fields keys with
  | None -> 0.0
  | Some (key, value) -> parse_float_value ~line_no:record.line_no ~key value

let boolish_false value =
  match String.lowercase_ascii (trim value) with
  | "0" | "false" | "no" | "n" | "failed" | "fail" | "error" -> true
  | _ -> false

let record_failed record =
  match first_present record.fields [ "passed"; "ok"; "success"; "status"; "result" ] with
  | None -> false
  | Some (_, value) -> boolish_false value

let add_first_string fields keys acc =
  match first_present fields keys with
  | None -> acc
  | Some (_, value) ->
      let normalized = trim value in
      if normalized = "" then acc else StringSet.add normalized acc

type summary = {
  records : int;
  chain_hash : string;
  total_tokens_in : int;
  total_tokens_out : int;
  total_cost_usd : float;
  latency_ms : float list;
  leakage_hits : int;
  failures : int;
  samples : int;
  dataset_hashes : StringSet.t;
  prompt_hashes : StringSet.t;
  model_ids : StringSet.t;
  regions : StringSet.t;
}

let empty_summary =
  {
    records = 0;
    chain_hash = String.make 64 '0';
    total_tokens_in = 0;
    total_tokens_out = 0;
    total_cost_usd = 0.0;
    latency_ms = [];
    leakage_hits = 0;
    failures = 0;
    samples = 0;
    dataset_hashes = StringSet.empty;
    prompt_hashes = StringSet.empty;
    model_ids = StringSet.empty;
    regions = StringSet.empty;
  }

let summarize_record summary record =
  let latency = float_field record [ "latency_ms"; "duration_ms"; "elapsed_ms" ] in
  let latency_values = if latency > 0.0 then latency :: summary.latency_ms else summary.latency_ms in
  let samples = int_field record [ "n"; "samples"; "sample_count"; "rows" ] in
  {
    records = summary.records + 1;
    chain_hash = record.chain_hash;
    total_tokens_in = summary.total_tokens_in + int_field record [ "tokens_in"; "input_tokens"; "prompt_tokens" ];
    total_tokens_out = summary.total_tokens_out + int_field record [ "tokens_out"; "output_tokens"; "completion_tokens" ];
    total_cost_usd = summary.total_cost_usd +. float_field record [ "cost_usd"; "usd" ];
    latency_ms = latency_values;
    leakage_hits = summary.leakage_hits + int_field record [ "leakage_hits"; "leakage_count"; "pii_hits"; "secret_hits" ];
    failures = summary.failures + if record_failed record then 1 else 0;
    samples = summary.samples + if samples > 0 then samples else 1;
    dataset_hashes = add_first_string record.fields [ "dataset_sha256"; "dataset_hash"; "corpus_hash" ] summary.dataset_hashes;
    prompt_hashes = add_first_string record.fields [ "prompt_sha256"; "prompt_hash"; "template_hash" ] summary.prompt_hashes;
    model_ids = add_first_string record.fields [ "model"; "model_id"; "provider_model" ] summary.model_ids;
    regions = add_first_string record.fields [ "region"; "data_region"; "residency" ] summary.regions;
  }

let summarize records = List.fold_left summarize_record empty_summary records

let percentile fraction values =
  match values with
  | [] -> 0.0
  | _ ->
      let sorted = Array.of_list values in
      Array.sort compare sorted;
      let count = Array.length sorted in
      let raw = int_of_float (ceil (fraction *. float_of_int count)) - 1 in
      let index = max 0 (min (count - 1) raw) in
      sorted.(index)

let failure_rate summary =
  if summary.records = 0 then 0.0 else float_of_int summary.failures /. float_of_int summary.records

let set_to_string set =
  if StringSet.is_empty set then "-" else String.concat "," (StringSet.elements set)

let print_summary label summary =
  Printf.printf "%s.records=%d\n" label summary.records;
  Printf.printf "%s.samples=%d\n" label summary.samples;
  Printf.printf "%s.chain_hash=%s\n" label summary.chain_hash;
  Printf.printf "%s.tokens_in=%d\n" label summary.total_tokens_in;
  Printf.printf "%s.tokens_out=%d\n" label summary.total_tokens_out;
  Printf.printf "%s.cost_usd=%.8f\n" label summary.total_cost_usd;
  Printf.printf "%s.latency_p50_ms=%.3f\n" label (percentile 0.50 summary.latency_ms);
  Printf.printf "%s.latency_p95_ms=%.3f\n" label (percentile 0.95 summary.latency_ms);
  Printf.printf "%s.leakage_hits=%d\n" label summary.leakage_hits;
  Printf.printf "%s.failures=%d\n" label summary.failures;
  Printf.printf "%s.failure_rate=%.6f\n" label (failure_rate summary);
  Printf.printf "%s.dataset_hashes=%s\n" label (set_to_string summary.dataset_hashes);
  Printf.printf "%s.prompt_hashes=%s\n" label (set_to_string summary.prompt_hashes);
  Printf.printf "%s.models=%s\n" label (set_to_string summary.model_ids);
  Printf.printf "%s.regions=%s\n" label (set_to_string summary.regions)

type check = { name : string; ok : bool; detail : string }

type gate_config = {
  baseline_path : string;
  candidate_path : string;
  ignore_keys : StringSet.t;
  max_cost_ratio : float;
  max_p95_latency_ratio : float;
  max_failure_rate : float;
  max_leakage_hits : int;
  min_record_ratio : float;
  require_same_dataset : bool;
  require_same_prompt : bool;
  require_same_model : bool;
  require_same_region : bool;
}

let ratio ~baseline ~candidate =
  if baseline <= 0.0 then if candidate <= 0.0 then 1.0 else 1.0 /. 0.0 else candidate /. baseline

let check_ratio name limit baseline candidate unit =
  let observed = ratio ~baseline ~candidate in
  {
    name;
    ok = observed <= limit;
    detail = Printf.sprintf "observed=%.6f limit=%.6f baseline=%.6f%s candidate=%.6f%s" observed limit baseline unit candidate unit;
  }

let check_int_max name limit observed =
  { name; ok = observed <= limit; detail = Printf.sprintf "observed=%d limit=%d" observed limit }

let check_float_max name limit observed =
  { name; ok = observed <= limit; detail = Printf.sprintf "observed=%.6f limit=%.6f" observed limit }

let check_set_equal name left right =
  {
    name;
    ok = StringSet.equal left right;
    detail = Printf.sprintf "baseline=%s candidate=%s" (set_to_string left) (set_to_string right);
  }

let print_check check =
  Printf.printf "%s %s %s\n" (if check.ok then "PASS" else "FAIL") check.name check.detail

let run_gate config =
  let baseline = read_ledger ~ignore_keys:config.ignore_keys config.baseline_path |> summarize in
  let candidate = read_ledger ~ignore_keys:config.ignore_keys config.candidate_path |> summarize in
  if baseline.records = 0 then user_error "baseline ledger has no records";
  if candidate.records = 0 then user_error "candidate ledger has no records";
  print_summary "baseline" baseline;
  print_summary "candidate" candidate;
  let checks = ref [] in
  let add check = checks := check :: !checks in
  add (check_ratio "cost_ratio" config.max_cost_ratio baseline.total_cost_usd candidate.total_cost_usd "usd");
  add (check_ratio "latency_p95_ratio" config.max_p95_latency_ratio (percentile 0.95 baseline.latency_ms) (percentile 0.95 candidate.latency_ms) "ms");
  add (check_float_max "failure_rate" config.max_failure_rate (failure_rate candidate));
  add (check_int_max "leakage_hits" config.max_leakage_hits candidate.leakage_hits);
  add
    {
      name = "record_coverage";
      ok = ratio ~baseline:(float_of_int baseline.records) ~candidate:(float_of_int candidate.records) >= config.min_record_ratio;
      detail =
        Printf.sprintf "observed=%.6f limit=%.6f baseline=%d candidate=%d"
          (ratio ~baseline:(float_of_int baseline.records) ~candidate:(float_of_int candidate.records))
          config.min_record_ratio baseline.records candidate.records;
    };
  if config.require_same_dataset then add (check_set_equal "dataset_hashes" baseline.dataset_hashes candidate.dataset_hashes);
  if config.require_same_prompt then add (check_set_equal "prompt_hashes" baseline.prompt_hashes candidate.prompt_hashes);
  if config.require_same_model then add (check_set_equal "models" baseline.model_ids candidate.model_ids);
  if config.require_same_region then add (check_set_equal "regions" baseline.regions candidate.regions);
  let ordered = List.rev !checks in
  List.iter print_check ordered;
  List.for_all (fun check -> check.ok) ordered

let parse_float_arg name value =
  try float_of_string value with Failure _ -> user_error (Printf.sprintf "%s expects a number, got %S" name value)

let parse_int_arg name value =
  try int_of_string value with Failure _ -> user_error (Printf.sprintf "%s expects an integer, got %S" name value)

let add_ignore_csv ignore_keys value =
  split_csv value
  |> List.map normalize_key
  |> List.fold_left (fun acc key -> if key = "" then acc else StringSet.add key acc) ignore_keys

let usage =
  String.concat "\n"
    [
      "Usage:";
      "  ocaml EvalArtifactLineageGate.ml selftest";
      "  ocaml EvalArtifactLineageGate.ml summarize --input ledger.tsv [--ignore key1,key2]";
      "  ocaml EvalArtifactLineageGate.ml chain --input ledger.tsv [--canonical] [--ignore key1,key2]";
      "  ocaml EvalArtifactLineageGate.ml gate --baseline baseline.tsv --candidate candidate.tsv [options]";
      "";
      "Ledger format: one record per line, key=value fields separated by tabs or spaces.";
      "Percent-encode tabs, spaces, newlines, and secrets before writing values.";
      "Useful fields: dataset_sha256, prompt_sha256, model, region, tokens_in, tokens_out,";
      "cost_usd, latency_ms, leakage_hits, n, passed.";
      "";
      "Gate options:";
      "  --max-cost-ratio N            default 1.15";
      "  --max-p95-latency-ratio N     default 1.25";
      "  --max-failure-rate N          default 0.0";
      "  --max-leakage-hits N          default 0";
      "  --min-record-ratio N          default 1.0";
      "  --require-same-dataset";
      "  --require-same-prompt";
      "  --require-same-model";
      "  --require-same-region";
    ]

let parse_input_args command args =
  let input = ref None in
  let ignore_keys = ref default_ignored_keys in
  let canonical = ref false in
  let rec loop = function
    | [] -> ()
    | "--input" :: value :: rest ->
        input := Some value;
        loop rest
    | "--ignore" :: value :: rest ->
        ignore_keys := add_ignore_csv !ignore_keys value;
        loop rest
    | "--canonical" :: rest when command = "chain" ->
        canonical := true;
        loop rest
    | value :: rest when !input = None && String.length value > 0 && value.[0] <> '-' ->
        input := Some value;
        loop rest
    | option :: _ -> user_error (Printf.sprintf "%s: unknown or incomplete option %S" command option)
  in
  loop args;
  match !input with
  | None -> user_error (command ^ " requires --input")
  | Some path -> (path, !ignore_keys, !canonical)

let parse_gate_args args =
  let baseline = ref None in
  let candidate = ref None in
  let ignore_keys = ref default_ignored_keys in
  let max_cost_ratio = ref 1.15 in
  let max_p95_latency_ratio = ref 1.25 in
  let max_failure_rate = ref 0.0 in
  let max_leakage_hits = ref 0 in
  let min_record_ratio = ref 1.0 in
  let require_same_dataset = ref false in
  let require_same_prompt = ref false in
  let require_same_model = ref false in
  let require_same_region = ref false in
  let rec loop = function
    | [] -> ()
    | "--baseline" :: value :: rest -> baseline := Some value; loop rest
    | "--candidate" :: value :: rest -> candidate := Some value; loop rest
    | "--ignore" :: value :: rest -> ignore_keys := add_ignore_csv !ignore_keys value; loop rest
    | "--max-cost-ratio" :: value :: rest -> max_cost_ratio := parse_float_arg "--max-cost-ratio" value; loop rest
    | "--max-p95-latency-ratio" :: value :: rest -> max_p95_latency_ratio := parse_float_arg "--max-p95-latency-ratio" value; loop rest
    | "--max-failure-rate" :: value :: rest -> max_failure_rate := parse_float_arg "--max-failure-rate" value; loop rest
    | "--max-leakage-hits" :: value :: rest -> max_leakage_hits := parse_int_arg "--max-leakage-hits" value; loop rest
    | "--min-record-ratio" :: value :: rest -> min_record_ratio := parse_float_arg "--min-record-ratio" value; loop rest
    | "--require-same-dataset" :: rest -> require_same_dataset := true; loop rest
    | "--require-same-prompt" :: rest -> require_same_prompt := true; loop rest
    | "--require-same-model" :: rest -> require_same_model := true; loop rest
    | "--require-same-region" :: rest -> require_same_region := true; loop rest
    | option :: _ -> user_error (Printf.sprintf "gate: unknown or incomplete option %S" option)
  in
  loop args;
  let require name = function Some value -> value | None -> user_error ("gate requires " ^ name) in
  {
    baseline_path = require "--baseline" !baseline;
    candidate_path = require "--candidate" !candidate;
    ignore_keys = !ignore_keys;
    max_cost_ratio = !max_cost_ratio;
    max_p95_latency_ratio = !max_p95_latency_ratio;
    max_failure_rate = !max_failure_rate;
    max_leakage_hits = !max_leakage_hits;
    min_record_ratio = !min_record_ratio;
    require_same_dataset = !require_same_dataset;
    require_same_prompt = !require_same_prompt;
    require_same_model = !require_same_model;
    require_same_region = !require_same_region;
  }

let run_selftest () =
  let vectors =
    [
      ("", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
      ("abc", "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    ]
  in
  let failed =
    List.filter
      (fun (input, expected) ->
        let actual = Sha256.hex input in
        if actual = expected then false
        else (
          Printf.eprintf "sha256 selftest failed for %S: expected %s got %s\n" input expected actual;
          true))
      vectors
  in
  if failed = [] then print_endline "selftest=ok" else exit 1

let run_summarize args =
  let path, ignore_keys, _ = parse_input_args "summarize" args in
  let summary = read_ledger ~ignore_keys path |> summarize in
  print_summary "ledger" summary

let run_chain args =
  let path, ignore_keys, show_canonical = parse_input_args "chain" args in
  read_ledger ~ignore_keys path
  |> List.iter (fun record ->
         Printf.printf "line=%d record_hash=%s prev_chain_hash=%s chain_hash=%s\n" record.line_no record.record_hash record.previous_chain_hash record.chain_hash;
         if show_canonical then Printf.printf "canonical_begin\n%s\ncanonical_end\n" record.canonical)

let run_gate_command args =
  let ok = parse_gate_args args |> run_gate in
  if ok then (print_endline "GATE PASS"; exit 0) else (print_endline "GATE FAIL"; exit 1)

let main () =
  let args = match Array.to_list Sys.argv with _program :: rest -> rest | [] -> [] in
  try
    match args with
    | [ "selftest" ] -> run_selftest ()
    | "summarize" :: rest -> run_summarize rest
    | "chain" :: rest -> run_chain rest
    | "gate" :: rest -> run_gate_command rest
    | _ -> prerr_endline usage; exit 2
  with
  | User_error message -> Printf.eprintf "error: %s\n\n%s\n" message usage; exit 2
  | Sys_error message -> Printf.eprintf "io-error: %s\n" message; exit 2

let () = main ()

(*
This solves the April 2026 problem where LLM evaluation, prompt regression testing, model routing, and AI CI gates produce many small artifacts but very little trustworthy provenance. Built because I keep seeing teams store raw prompts, screenshots, and loose JSON reports, then nobody can prove which dataset hash, prompt hash, model id, region, token cost, latency, or leakage result actually shipped. Use it when a repo needs a lightweight AI eval ledger, reproducible machine learning evidence trail, prompt drift gate, model regression checker, or CI guard for private inference pipelines without adding a database or a cloud service. The trick: every key=value record is canonicalized, volatile fields are ignored, SHA-256 is computed in plain OCaml, and the records are chained so a changed line changes every later chain hash. Drop this into any GitHub Actions, Buildkite, Jenkins, local research harness, edge inference worker, or DevOps release script that needs searchable terms like LLM evaluation ledger, AI provenance, prompt hash audit, model quality gate, data residency proof, token cost regression, and reproducible eval artifacts while keeping secrets out of the repository.
*)
