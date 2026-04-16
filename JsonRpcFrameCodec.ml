(* Incremental Content-Length framed JSON-RPC codec for MCP and LSP style transports. *)

type decode_error =
  | Closed_decoder
  | Header_too_large of int
  | Body_too_large of int
  | Too_many_headers of int
  | Missing_content_length
  | Duplicate_header of string
  | Invalid_header_name of string
  | Malformed_header_line of string
  | Invalid_content_length of string
  | Unsupported_charset of string
  | Incomplete_frame_at_eof

type config = {
  max_header_bytes : int;
  max_body_bytes : int;
  max_headers : int;
  initial_buffer_size : int;
  default_content_type : string;
  accepted_charsets : string list;
}

let default_config =
  {
    max_header_bytes = 16 * 1024;
    max_body_bytes = 16 * 1024 * 1024;
    max_headers = 64;
    initial_buffer_size = 4096;
    default_content_type = "application/vscode-jsonrpc; charset=utf-8";
    accepted_charsets = [ "utf-8"; "utf8" ];
  }

type frame = {
  headers : (string * string) list;
  content_length : int;
  content_type : string option;
  body : bytes;
}

type decoder = {
  config : config;
  mutable buffer : bytes;
  mutable used : int;
  mutable closed : bool;
}

type parsed_headers = {
  headers : (string * string) list;
  content_length : int;
  content_type : string option;
}

let ( let* ) value f =
  match value with
  | Ok item -> f item
  | Error _ as error -> error

let string_for_all predicate value =
  let rec loop index =
    if index = String.length value then true
    else if predicate value.[index] then loop (index + 1)
    else false
  in
  loop 0

let string_ends_with ~suffix value =
  let suffix_length = String.length suffix in
  let value_length = String.length value in
  value_length >= suffix_length
  && String.sub value (value_length - suffix_length) suffix_length = suffix

let drop_trailing_cr value =
  if value <> "" && string_ends_with ~suffix:"\r" value then
    String.sub value 0 (String.length value - 1)
  else
    value

let unquote value =
  let value = String.trim value in
  let length = String.length value in
  if length >= 2 && value.[0] = '"' && value.[length - 1] = '"' then
    String.sub value 1 (length - 2)
  else
    value

let normalize_header_name name =
  String.lowercase_ascii (String.trim name)

let is_header_token_char = function
  | 'a' .. 'z' | 'A' .. 'Z' | '0' .. '9' | '-' -> true
  | _ -> false

let is_supported_charset config value =
  let lowered = String.lowercase_ascii value in
  List.exists (fun candidate -> String.lowercase_ascii candidate = lowered) config.accepted_charsets

let pp_decode_error formatter = function
  | Closed_decoder ->
      Format.pp_print_string formatter "decoder is already closed"
  | Header_too_large size ->
      Format.fprintf formatter "header block exceeds configured limit (%d bytes)" size
  | Body_too_large size ->
      Format.fprintf formatter "frame body exceeds configured limit (%d bytes)" size
  | Too_many_headers count ->
      Format.fprintf formatter "header block contains too many headers (%d)" count
  | Missing_content_length ->
      Format.pp_print_string formatter "missing Content-Length header"
  | Duplicate_header name ->
      Format.fprintf formatter "duplicate %s header" name
  | Invalid_header_name name ->
      Format.fprintf formatter "invalid header name: %S" name
  | Malformed_header_line line ->
      Format.fprintf formatter "malformed header line: %S" line
  | Invalid_content_length value ->
      Format.fprintf formatter "invalid Content-Length value: %S" value
  | Unsupported_charset value ->
      Format.fprintf formatter "unsupported Content-Type charset: %S" value
  | Incomplete_frame_at_eof ->
      Format.pp_print_string formatter "incomplete frame left in decoder at end of stream"

let decode_error_to_string error =
  Format.asprintf "%a" pp_decode_error error

let create ?(config = default_config) () =
  let initial_size = max 128 config.initial_buffer_size in
  {
    config;
    buffer = Bytes.create initial_size;
    used = 0;
    closed = false;
  }

let reset decoder =
  decoder.used <- 0;
  decoder.closed <- false

let buffered_bytes decoder =
  decoder.used

let is_closed decoder =
  decoder.closed

let has_pending_bytes decoder =
  decoder.used > 0

let next_capacity current needed =
  let rec grow capacity =
    if capacity >= needed then capacity else grow (capacity * 2)
  in
  grow (max 128 current)

let ensure_capacity decoder additional =
  let needed = decoder.used + additional in
  if needed > Bytes.length decoder.buffer then (
    let replacement = Bytes.create (next_capacity (Bytes.length decoder.buffer) needed) in
    Bytes.blit decoder.buffer 0 replacement 0 decoder.used;
    decoder.buffer <- replacement)

let discard_prefix decoder count =
  let remaining = decoder.used - count in
  if remaining > 0 then Bytes.blit decoder.buffer count decoder.buffer 0 remaining;
  decoder.used <- remaining

let parse_content_length config raw_value =
  let value = String.trim raw_value in
  match int_of_string_opt value with
  | None -> Error (Invalid_content_length raw_value)
  | Some length when length < 0 -> Error (Invalid_content_length raw_value)
  | Some length when length > config.max_body_bytes -> Error (Body_too_large length)
  | Some length -> Ok length

let validate_content_type config raw_value =
  let value = String.trim raw_value in
  let parameters = String.split_on_char ';' value in
  let rec find_charset = function
    | [] -> Ok value
    | segment :: rest ->
        let segment = String.trim segment in
        begin
          match String.index_opt segment '=' with
          | None -> find_charset rest
          | Some index ->
              let name = String.sub segment 0 index |> String.trim |> String.lowercase_ascii in
              let param_value =
                String.sub segment (index + 1) (String.length segment - index - 1)
                |> unquote
                |> String.trim
              in
              if name = "charset" then
                if is_supported_charset config param_value then Ok value
                else Error (Unsupported_charset param_value)
              else
                find_charset rest
        end
  in
  find_charset parameters

let parse_header_line line =
  match String.index_opt line ':' with
  | None -> Error (Malformed_header_line line)
  | Some separator_index ->
      let name = String.sub line 0 separator_index |> String.trim in
      if name = "" then Error (Malformed_header_line line)
      else if not (string_for_all is_header_token_char name) then
        Error (Invalid_header_name name)
      else
        let value =
          String.sub line (separator_index + 1) (String.length line - separator_index - 1)
          |> String.trim
        in
        Ok (name, value)

let parse_headers config header_block =
  let lines =
    if header_block = "" then []
    else
      String.split_on_char '\n' header_block
      |> List.map drop_trailing_cr
      |> List.filter (fun line -> String.trim line <> "")
  in
  let header_count = List.length lines in
  if header_count > config.max_headers then Error (Too_many_headers header_count)
  else
    let rec loop remaining headers content_length content_type =
      match remaining with
      | [] -> (
          match content_length with
          | None -> Error Missing_content_length
          | Some length ->
              Ok
                {
                  headers = List.rev headers;
                  content_length = length;
                  content_type;
                })
      | line :: tail ->
          let* (name, value) = parse_header_line line in
          let normalized_name = normalize_header_name name in
          begin
            match normalized_name with
            | "content-length" -> (
                match content_length with
                | Some _ -> Error (Duplicate_header normalized_name)
                | None ->
                    let* parsed_length = parse_content_length config value in
                    loop tail ((name, value) :: headers) (Some parsed_length) content_type)
            | "content-type" -> (
                match content_type with
                | Some _ -> Error (Duplicate_header normalized_name)
                | None ->
                    let* parsed_type = validate_content_type config value in
                    loop tail ((name, value) :: headers) content_length (Some parsed_type))
            | _ ->
                loop tail ((name, value) :: headers) content_length content_type
          end
    in
    loop lines [] None None

let find_header_terminator buffer used =
  let rec scan index =
    if index + 1 >= used then None
    else if
      index + 3 < used
      && Bytes.get buffer index = '\r'
      && Bytes.get buffer (index + 1) = '\n'
      && Bytes.get buffer (index + 2) = '\r'
      && Bytes.get buffer (index + 3) = '\n'
    then
      Some (index, 4)
    else if Bytes.get buffer index = '\n' && Bytes.get buffer (index + 1) = '\n' then
      Some (index, 2)
    else
      scan (index + 1)
  in
  scan 0

let rec drain_frames decoder accumulator =
  match find_header_terminator decoder.buffer decoder.used with
  | None ->
      if decoder.used > decoder.config.max_header_bytes then
        Error (Header_too_large decoder.used)
      else
        Ok (List.rev accumulator)
  | Some (header_end, delimiter_length) ->
      if header_end > decoder.config.max_header_bytes then
        Error (Header_too_large header_end)
      else
        let header_block = Bytes.sub_string decoder.buffer 0 header_end in
        let* parsed_headers = parse_headers decoder.config header_block in
        let frame_size = header_end + delimiter_length + parsed_headers.content_length in
        if decoder.used < frame_size then
          Ok (List.rev accumulator)
        else
          let body_offset = header_end + delimiter_length in
          let body = Bytes.sub decoder.buffer body_offset parsed_headers.content_length in
          let frame =
            {
              headers = parsed_headers.headers;
              content_length = parsed_headers.content_length;
              content_type = parsed_headers.content_type;
              body;
            }
          in
          discard_prefix decoder frame_size;
          drain_frames decoder (frame :: accumulator)

let feed_bytes decoder chunk offset length =
  if decoder.closed then Error Closed_decoder
  else if offset < 0 || length < 0 || offset > Bytes.length chunk - length then
    invalid_arg "feed_bytes"
  else if length = 0 then
    Ok []
  else (
    ensure_capacity decoder length;
    Bytes.blit chunk offset decoder.buffer decoder.used length;
    decoder.used <- decoder.used + length;
    drain_frames decoder [])

let feed_string decoder chunk =
  feed_bytes decoder (Bytes.of_string chunk) 0 (String.length chunk)

let close decoder =
  let* frames = drain_frames decoder [] in
  decoder.closed <- true;
  if decoder.used = 0 then Ok frames else Error Incomplete_frame_at_eof

let decode_all ?(config = default_config) chunk =
  let decoder = create ~config () in
  let* frames = feed_string decoder chunk in
  let* trailing_frames = close decoder in
  Ok (frames @ trailing_frames)

let header frame name =
  let needle = normalize_header_name name in
  let rec scan = function
    | [] -> None
    | (candidate_name, value) :: rest ->
        if normalize_header_name candidate_name = needle then Some value else scan rest
  in
  scan frame.headers

let body_text frame =
  Bytes.to_string frame.body

let body_preview ?(limit = 256) frame =
  let body = body_text frame in
  if String.length body <= limit then body else String.sub body 0 limit ^ "..."

let fnv_offset_basis = 0xcbf29ce484222325L
let fnv_prime = 0x100000001b3L

let fnv1a_string hash value =
  let state = ref hash in
  for index = 0 to String.length value - 1 do
    let code = Int64.of_int (Char.code value.[index]) in
    state := Int64.mul (Int64.logxor !state code) fnv_prime
  done;
  !state

let fnv1a_bytes hash value =
  let state = ref hash in
  for index = 0 to Bytes.length value - 1 do
    let code = Int64.of_int (Char.code (Bytes.get value index)) in
    state := Int64.mul (Int64.logxor !state code) fnv_prime
  done;
  !state

let frame_fingerprint64 frame =
  let header_hash =
    List.fold_left
      (fun hash (name, value) ->
        let hash = fnv1a_string hash (normalize_header_name name) in
        let hash = fnv1a_string hash ":" in
        let hash = fnv1a_string hash value in
        fnv1a_string hash "\n")
      fnv_offset_basis
      frame.headers
  in
  fnv1a_bytes header_hash frame.body

let frame_fingerprint_hex frame =
  Printf.sprintf "%016Lx" (frame_fingerprint64 frame)

let strip_reserved_headers headers =
  List.filter
    (fun (name, _) ->
      let normalized = normalize_header_name name in
      normalized <> "content-length" && normalized <> "content-type")
    headers

let render_headers headers =
  let rendered =
    List.map (fun (name, value) -> name ^ ": " ^ value) headers |> String.concat "\r\n"
  in
  rendered ^ "\r\n\r\n"

let encode_bytes ?content_type ?(headers = []) body =
  let body_length = Bytes.length body in
  let outgoing_headers =
    match content_type with
    | None -> [ ("Content-Length", string_of_int body_length) ]
    | Some value ->
        [ ("Content-Length", string_of_int body_length); ("Content-Type", value) ]
  in
  let outgoing_headers = outgoing_headers @ strip_reserved_headers headers in
  let header_block = render_headers outgoing_headers |> Bytes.of_string in
  let frame = Bytes.create (Bytes.length header_block + body_length) in
  Bytes.blit header_block 0 frame 0 (Bytes.length header_block);
  Bytes.blit body 0 frame (Bytes.length header_block) body_length;
  frame

let encode_string ?content_type ?headers body =
  encode_bytes ?content_type ?headers (Bytes.of_string body)

let encode_jsonrpc_bytes ?(headers = []) body =
  encode_bytes ~content_type:default_config.default_content_type ~headers body

let encode_jsonrpc_string ?(headers = []) body =
  encode_string ~content_type:default_config.default_content_type ~headers body

let fold_frames decoder input ~init ~f =
  let* frames = feed_string decoder input in
  Ok (List.fold_left f init frames)

(*
This solves broken MCP stdio framing and JSON-RPC Content-Length parsing in OCaml apps, especially when an MCP server, MCP client, LSP bridge, or AI agent runtime reads partial chunks from stdin or a pipe. Built because the real failures in 2026 are still the boring ones: split headers, mixed line endings, oversized payloads, duplicate Content-Length values, and retries that look like new messages. Use it when you need a strict OCaml MCP parser or OCaml JSON-RPC frame codec that can sit between your transport and your JSON layer without pulling in extra packages. The trick: it parses incrementally, blocks the ambiguous header cases that cause ghost tool calls, accepts normal UTF-8 content types, and gives you a stable frame fingerprint for logs or dedupe logic. Drop this into a dune project, wire feed_bytes to your reader loop, use encode_string for writes, and let your existing JSON code handle the body. I wrote it to be easy to fork, easy to audit, and easy to find on GitHub or Google for searches like OCaml MCP stdio parser, OCaml JSON-RPC Content-Length decoder, or OCaml LSP frame codec.
*)
