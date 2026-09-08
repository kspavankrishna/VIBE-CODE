(* CI-friendly compatibility gate for MCP tool manifests and JSON Schema contracts. *)

module StringMap = Map.Make (String)
module StringSet = Set.Make (String)

type json =
  | JNull
  | JBool of bool
  | JNumber of string
  | JString of string
  | JArray of json list
  | JObject of (string * json) list

type severity =
  | Breaking
  | Non_breaking
  | Warning
  | Note

type unsupported_policy =
  | Fail
  | Warn
  | Ignore

type issue = {
  severity : severity;
  tool : string option;
  surface : string;
  path : string;
  code : string;
  message : string;
}

type location = {
  index : int;
  line : int;
  column : int;
}

exception Json_parse_error of location * string
exception Cli_error of string
exception Help_requested
exception Manifest_error of string
exception Resolve_error of string

let starts_with prefix value =
  let prefix_length = String.length prefix in
  let value_length = String.length value in
  value_length >= prefix_length && String.sub value 0 prefix_length = prefix

let string_set_of_list values =
  List.fold_left (fun acc value -> StringSet.add value acc) StringSet.empty values

let compare_object_field (left_name, _) (right_name, _) =
  String.compare left_name right_name

let rec json_equal left right =
  match left, right with
  | JNull, JNull -> true
  | JBool left_value, JBool right_value -> left_value = right_value
  | JNumber left_value, JNumber right_value -> left_value = right_value
  | JString left_value, JString right_value -> left_value = right_value
  | JArray left_items, JArray right_items ->
      List.length left_items = List.length right_items
      && List.for_all2 json_equal left_items right_items
  | JObject left_fields, JObject right_fields ->
      let left_fields = List.sort compare_object_field left_fields in
      let right_fields = List.sort compare_object_field right_fields in
      List.length left_fields = List.length right_fields
      && List.for_all2
           (fun (left_name, left_value) (right_name, right_value) ->
             left_name = right_name && json_equal left_value right_value)
           left_fields
           right_fields
  | _ -> false

let severity_rank = function
  | Breaking -> 0
  | Warning -> 1
  | Non_breaking -> 2
  | Note -> 3

let severity_name = function
  | Breaking -> "breaking"
  | Warning -> "warning"
  | Non_breaking -> "non-breaking"
  | Note -> "note"

let unsupported_severity = function
  | Fail -> Breaking
  | Warn -> Warning
  | Ignore -> Note

let field name fields =
  try Some (List.assoc name fields) with
  | Not_found -> None

let rec first_field names fields =
  match names with
  | [] -> None
  | name :: rest -> (
      match field name fields with
      | Some value -> Some value
      | None -> first_field rest fields)

let json_string = function
  | JString value -> Some value
  | _ -> None

let json_bool = function
  | JBool value -> Some value
  | _ -> None

let string_list_of_json = function
  | JArray items ->
      List.fold_right
        (fun item acc ->
          match item, acc with
          | JString value, Some values -> Some (value :: values)
          | _ -> None)
        items
        (Some [])
  | _ -> None

let rec escape_json_string buffer value index =
  if index = String.length value then ()
  else
    let ch = value.[index] in
    begin
      match ch with
      | '"' -> Buffer.add_string buffer "\\\""
      | '\\' -> Buffer.add_string buffer "\\\\"
      | '\b' -> Buffer.add_string buffer "\\b"
      | '\012' -> Buffer.add_string buffer "\\f"
      | '\n' -> Buffer.add_string buffer "\\n"
      | '\r' -> Buffer.add_string buffer "\\r"
      | '\t' -> Buffer.add_string buffer "\\t"
      | _ when Char.code ch < 0x20 ->
          Buffer.add_string buffer (Printf.sprintf "\\u%04x" (Char.code ch))
      | _ -> Buffer.add_char buffer ch
    end;
    escape_json_string buffer value (index + 1)

let json_to_string value =
  let rec render buffer current =
    match current with
    | JNull -> Buffer.add_string buffer "null"
    | JBool true -> Buffer.add_string buffer "true"
    | JBool false -> Buffer.add_string buffer "false"
    | JNumber number -> Buffer.add_string buffer number
    | JString string_value ->
        Buffer.add_char buffer '"';
        escape_json_string buffer string_value 0;
        Buffer.add_char buffer '"'
    | JArray items ->
        Buffer.add_char buffer '[';
        List.iteri
          (fun index item ->
            if index > 0 then Buffer.add_char buffer ',';
            render buffer item)
          items;
        Buffer.add_char buffer ']'
    | JObject fields ->
        Buffer.add_char buffer '{';
        List.iteri
          (fun index (name, item) ->
            if index > 0 then Buffer.add_char buffer ',';
            Buffer.add_char buffer '"';
            escape_json_string buffer name 0;
            Buffer.add_string buffer "\":";
            render buffer item)
          fields;
        Buffer.add_char buffer '}'
  in
  let buffer = Buffer.create 256 in
  render buffer value;
  Buffer.contents buffer

module Json = struct
  type parser = {
    text : string;
    length : int;
    mutable index : int;
    mutable line : int;
    mutable column : int;
  }

  let create text =
    { text; length = String.length text; index = 0; line = 1; column = 1 }

  let current_location parser =
    { index = parser.index; line = parser.line; column = parser.column }

  let fail parser message =
    raise (Json_parse_error (current_location parser, message))

  let peek parser =
    if parser.index >= parser.length then None else Some parser.text.[parser.index]

  let advance parser =
    match peek parser with
    | None -> None
    | Some ch ->
        parser.index <- parser.index + 1;
        if ch = '\n' then (
          parser.line <- parser.line + 1;
          parser.column <- 1)
        else
          parser.column <- parser.column + 1;
        Some ch

  let expect parser expected =
    match advance parser with
    | Some ch when ch = expected -> ()
    | Some ch ->
        fail parser
          (Printf.sprintf "expected %C but found %C" expected ch)
    | None ->
        fail parser
          (Printf.sprintf "expected %C but reached end of input" expected)

  let is_whitespace = function
    | ' ' | '\t' | '\n' | '\r' -> true
    | _ -> false

  let rec skip_whitespace parser =
    match peek parser with
    | Some ch when is_whitespace ch ->
        ignore (advance parser);
        skip_whitespace parser
    | _ -> ()

  let hex_value = function
    | '0' .. '9' as ch -> Char.code ch - Char.code '0'
    | 'a' .. 'f' as ch -> 10 + Char.code ch - Char.code 'a'
    | 'A' .. 'F' as ch -> 10 + Char.code ch - Char.code 'A'
    | _ -> -1

  let add_utf8 buffer codepoint =
    if codepoint < 0 || codepoint > 0x10ffff then
      invalid_arg "add_utf8"
    else if codepoint <= 0x7f then
      Buffer.add_char buffer (Char.chr codepoint)
    else if codepoint <= 0x7ff then (
      Buffer.add_char buffer (Char.chr (0xc0 lor (codepoint lsr 6)));
      Buffer.add_char buffer (Char.chr (0x80 lor (codepoint land 0x3f))))
    else if codepoint <= 0xffff then (
      Buffer.add_char buffer (Char.chr (0xe0 lor (codepoint lsr 12)));
      Buffer.add_char buffer (Char.chr (0x80 lor ((codepoint lsr 6) land 0x3f)));
      Buffer.add_char buffer (Char.chr (0x80 lor (codepoint land 0x3f))))
    else (
      Buffer.add_char buffer (Char.chr (0xf0 lor (codepoint lsr 18)));
      Buffer.add_char buffer (Char.chr (0x80 lor ((codepoint lsr 12) land 0x3f)));
      Buffer.add_char buffer (Char.chr (0x80 lor ((codepoint lsr 6) land 0x3f)));
      Buffer.add_char buffer (Char.chr (0x80 lor (codepoint land 0x3f))))

  let read_hex_quad parser =
    let digit offset =
      match advance parser with
      | Some ch ->
          let value = hex_value ch in
          if value < 0 then
            fail parser
              (Printf.sprintf "invalid hex digit %C in unicode escape" ch)
          else
            value lsl offset
      | None -> fail parser "incomplete unicode escape"
    in
    digit 12 lor digit 8 lor digit 4 lor digit 0

  let parse_unicode_escape parser buffer =
    let codepoint = read_hex_quad parser in
    if codepoint >= 0xd800 && codepoint <= 0xdbff then (
      expect parser '\\';
      expect parser 'u';
      let low = read_hex_quad parser in
      if low < 0xdc00 || low > 0xdfff then
        fail parser "invalid low surrogate in unicode escape";
      let combined =
        0x10000 + (((codepoint - 0xd800) lsl 10) lor (low - 0xdc00))
      in
      add_utf8 buffer combined)
    else if codepoint >= 0xdc00 && codepoint <= 0xdfff then
      fail parser "unexpected low surrogate in unicode escape"
    else
      add_utf8 buffer codepoint

  let parse_string parser =
    expect parser '"';
    let buffer = Buffer.create 64 in
    let rec loop () =
      match advance parser with
      | None -> fail parser "unterminated string"
      | Some '"' -> Buffer.contents buffer
      | Some '\\' -> (
          match advance parser with
          | Some '"' ->
              Buffer.add_char buffer '"';
              loop ()
          | Some '\\' ->
              Buffer.add_char buffer '\\';
              loop ()
          | Some '/' ->
              Buffer.add_char buffer '/';
              loop ()
          | Some 'b' ->
              Buffer.add_char buffer '\b';
              loop ()
          | Some 'f' ->
              Buffer.add_char buffer '\012';
              loop ()
          | Some 'n' ->
              Buffer.add_char buffer '\n';
              loop ()
          | Some 'r' ->
              Buffer.add_char buffer '\r';
              loop ()
          | Some 't' ->
              Buffer.add_char buffer '\t';
              loop ()
          | Some 'u' ->
              parse_unicode_escape parser buffer;
              loop ()
          | Some ch ->
              fail parser (Printf.sprintf "unsupported escape sequence \\%C" ch)
          | None -> fail parser "unterminated escape sequence")
      | Some ch when Char.code ch < 0x20 ->
          fail parser "control character inside string literal"
      | Some ch ->
          Buffer.add_char buffer ch;
          loop ()
    in
    loop ()

  let is_digit = function
    | '0' .. '9' -> true
    | _ -> false

  let consume_digits parser =
    let rec loop () =
      match peek parser with
      | Some ch when is_digit ch ->
          ignore (advance parser);
          loop ()
      | _ -> ()
    in
    loop ()

  let consume_required_digit parser =
    match peek parser with
    | Some ch when is_digit ch -> ignore (advance parser)
    | _ -> fail parser "expected decimal digit"

  let parse_number parser =
    let start = parser.index in
    begin
      match peek parser with
      | Some '-' -> ignore (advance parser)
      | _ -> ()
    end;
    begin
      match peek parser with
      | Some '0' -> ignore (advance parser)
      | Some ('1' .. '9') ->
          ignore (advance parser);
          consume_digits parser
      | _ -> fail parser "invalid number literal"
    end;
    begin
      match peek parser with
      | Some '.' ->
          ignore (advance parser);
          consume_required_digit parser;
          consume_digits parser
      | _ -> ()
    end;
    begin
      match peek parser with
      | Some ('e' | 'E') ->
          ignore (advance parser);
          begin
            match peek parser with
            | Some ('+' | '-') -> ignore (advance parser)
            | _ -> ()
          end;
          consume_required_digit parser;
          consume_digits parser
      | _ -> ()
    end;
    JNumber (String.sub parser.text start (parser.index - start))

  let parse_literal parser literal value =
    let literal_length = String.length literal in
    for index = 0 to literal_length - 1 do
      match advance parser with
      | Some ch when ch = literal.[index] -> ()
      | Some ch ->
          fail parser
            (Printf.sprintf "expected %C while parsing %s but found %C"
               literal.[index]
               literal
               ch)
      | None -> fail parser (Printf.sprintf "unexpected end while parsing %s" literal)
    done;
    value

  let rec parse_value parser =
    skip_whitespace parser;
    match peek parser with
    | Some '"' -> JString (parse_string parser)
    | Some '{' -> parse_object parser
    | Some '[' -> parse_array parser
    | Some 't' -> parse_literal parser "true" (JBool true)
    | Some 'f' -> parse_literal parser "false" (JBool false)
    | Some 'n' -> parse_literal parser "null" JNull
    | Some ('-' | '0' .. '9') -> parse_number parser
    | Some ch ->
        fail parser
          (Printf.sprintf "unexpected character %C while parsing JSON value" ch)
    | None -> fail parser "unexpected end of input while parsing JSON value"

  and parse_object parser =
    expect parser '{';
    skip_whitespace parser;
    match peek parser with
    | Some '}' ->
        ignore (advance parser);
        JObject []
    | _ ->
        let rec loop acc =
          skip_whitespace parser;
          let key = parse_string parser in
          skip_whitespace parser;
          expect parser ':';
          let value = parse_value parser in
          skip_whitespace parser;
          match peek parser with
          | Some ',' ->
              ignore (advance parser);
              loop ((key, value) :: acc)
          | Some '}' ->
              ignore (advance parser);
              JObject (List.rev ((key, value) :: acc))
          | Some ch ->
              fail parser
                (Printf.sprintf
                   "expected ',' or '}' after object field but found %C"
                   ch)
          | None -> fail parser "unterminated object"
        in
        loop []

  and parse_array parser =
    expect parser '[';
    skip_whitespace parser;
    match peek parser with
    | Some ']' ->
        ignore (advance parser);
        JArray []
    | _ ->
        let rec loop acc =
          let value = parse_value parser in
          skip_whitespace parser;
          match peek parser with
          | Some ',' ->
              ignore (advance parser);
              loop (value :: acc)
          | Some ']' ->
              ignore (advance parser);
              JArray (List.rev (value :: acc))
          | Some ch ->
              fail parser
                (Printf.sprintf
                   "expected ',' or ']' after array element but found %C"
                   ch)
          | None -> fail parser "unterminated array"
        in
        loop []

  let parse text =
    let parser = create text in
    let value = parse_value parser in
    skip_whitespace parser;
    match peek parser with
    | None -> value
    | Some ch ->
        fail parser
          (Printf.sprintf "unexpected trailing character %C after JSON document" ch)
end

let read_file path =
  let input = open_in_bin path in
  Fun.protect
    ~finally:(fun () -> close_in_noerr input)
    (fun () ->
      let length = in_channel_length input in
      really_input_string input length)

let parse_json_file path =
  try Json.parse (read_file path) with
  | Json_parse_error (location, message) ->
      raise
        (Manifest_error
           (Printf.sprintf "%s:%d:%d: %s"
              path
              location.line
              location.column
              message))

let annotation_keys =
  string_set_of_list
    [
      "$comment";
      "description";
      "title";
      "examples";
      "example";
      "default";
      "deprecated";
      "readOnly";
      "writeOnly";
      "markdownDescription";
      "annotations";
    ]

let schema_combinator_keys =
  string_set_of_list
    [
      "allOf";
      "anyOf";
      "oneOf";
      "not";
      "if";
      "then";
      "else";
      "dependentSchemas";
      "dependencies";
      "patternProperties";
      "propertyNames";
      "unevaluatedProperties";
      "unevaluatedItems";
      "contains";
      "minContains";
      "maxContains";
      "prefixItems";
      "contentEncoding";
      "contentMediaType";
      "contentSchema";
      "$dynamicRef";
      "$dynamicAnchor";
    ]

let is_annotation_key key =
  StringSet.mem key annotation_keys || starts_with "x-" key

let rec strip_annotations value =
  match value with
  | JArray items -> JArray (List.map strip_annotations items)
  | JObject fields ->
      let filtered =
        fields
        |> List.filter (fun (name, _) -> not (is_annotation_key name))
        |> List.map (fun (name, item) -> (name, strip_annotations item))
      in
      JObject filtered
  | _ -> value

let semantic_json_equal left right =
  json_equal (strip_annotations left) (strip_annotations right)

let field_names fields =
  List.fold_left (fun acc (name, _) -> StringSet.add name acc) StringSet.empty fields

let string_of_string_set values =
  StringSet.elements values |> String.concat ", "

let root_path = "$"

let child_path base segment =
  if base = root_path then base ^ "." ^ segment else base ^ "." ^ segment

let index_path base index =
  Printf.sprintf "%s[%d]" base index

let merge_object_fields base_fields overlay_fields =
  let table = Hashtbl.create (List.length base_fields + List.length overlay_fields) in
  List.iter (fun (name, value) -> Hashtbl.replace table name value) base_fields;
  List.iter (fun (name, value) -> Hashtbl.replace table name value) overlay_fields;
  Hashtbl.fold (fun name value acc -> (name, value) :: acc) table []

let unescape_pointer_segment segment =
  let buffer = Buffer.create (String.length segment) in
  let rec loop index =
    if index = String.length segment then Buffer.contents buffer
    else if segment.[index] = '~' then
      if index + 1 >= String.length segment then
        raise (Resolve_error ("invalid JSON pointer segment: " ^ segment))
      else
        let replacement =
          match segment.[index + 1] with
          | '0' -> "~"
          | '1' -> "/"
          | ch ->
              raise
                (Resolve_error
                   (Printf.sprintf
                      "unsupported JSON pointer escape ~%c in segment %s"
                      ch
                      segment))
        in
        Buffer.add_string buffer replacement;
        loop (index + 2)
    else (
      Buffer.add_char buffer segment.[index];
      loop (index + 1))
  in
  loop 0

let resolve_pointer root pointer =
  if pointer = "#" then root
  else if starts_with "#/" pointer then
    let segments =
      String.sub pointer 2 (String.length pointer - 2)
      |> String.split_on_char '/'
      |> List.map unescape_pointer_segment
    in
    let rec descend current remaining =
      match remaining with
      | [] -> current
      | segment :: rest -> (
          match current with
          | JObject fields -> (
              match field segment fields with
              | Some value -> descend value rest
              | None ->
                  raise
                    (Resolve_error
                       (Printf.sprintf "JSON pointer %s is missing segment %s"
                          pointer
                          segment)))
          | JArray items -> (
              match int_of_string_opt segment with
              | Some index when index >= 0 && index < List.length items ->
                  descend (List.nth items index) rest
              | _ ->
                  raise
                    (Resolve_error
                       (Printf.sprintf
                          "JSON pointer %s cannot index array segment %s"
                          pointer
                          segment)))
          | _ ->
              raise
                (Resolve_error
                   (Printf.sprintf
                      "JSON pointer %s cannot descend through non-container value"
                      pointer)))
    in
    descend root segments
  else
    raise
      (Resolve_error
         (Printf.sprintf
            "only local JSON Schema references are supported, got %s"
            pointer))

let resolve_schema root schema =
  let rec loop seen current =
    match current with
    | JObject fields -> (
        match field "$ref" fields with
        | Some (JString pointer) ->
            if StringSet.mem pointer seen then
              raise
                (Resolve_error
                   (Printf.sprintf "cyclic JSON Schema reference detected at %s"
                      pointer));
            let target = resolve_pointer root pointer in
            let resolved_target = loop (StringSet.add pointer seen) target in
            let sibling_fields =
              List.filter (fun (name, _) -> name <> "$ref") fields
            in
            begin
              match resolved_target, sibling_fields with
              | JObject target_fields, _ :: _ ->
                  loop seen (JObject (merge_object_fields target_fields sibling_fields))
              | _, [] -> resolved_target
              | _ ->
                  raise
                    (Resolve_error
                       "cannot apply sibling keywords to a non-object $ref target")
            end
        | Some _ -> raise (Resolve_error "$ref must be a string")
        | None -> current)
    | _ -> current
  in
  loop StringSet.empty schema

let list_fold_lefti fn init items =
  let rec loop index acc remaining =
    match remaining with
    | [] -> acc
    | item :: rest -> loop (index + 1) (fn index acc item) rest
  in
  loop 0 init items

type manifest_format =
  | Auto
  | Mcp_tools
  | Openai_tools
  | Tool_map
  | Single_tool

type tool_spec = {
  name : string;
  description : string option;
  input_schema : json option;
  output_schema : json option;
  raw : json;
}

type manifest = {
  format_name : string;
  tools : tool_spec StringMap.t;
}

let looks_like_schema_object fields =
  let schemaish_keys =
    [
      "$ref";
      "$defs";
      "definitions";
      "type";
      "properties";
      "required";
      "additionalProperties";
      "items";
      "enum";
      "const";
      "minimum";
      "maximum";
      "minLength";
      "maxLength";
      "pattern";
      "minItems";
      "maxItems";
      "uniqueItems";
      "nullable";
      "format";
    ]
  in
  List.exists (fun key -> field key fields <> None) schemaish_keys

let first_string_field names fields =
  match first_field names fields with
  | Some (JString value) -> Some value
  | _ -> None

let first_schema_field names fields =
  first_field names fields

let build_tool_spec ?fallback_name json =
  let make_tool name description input_schema output_schema raw =
    { name; description; input_schema; output_schema; raw }
  in
  let rec from_json ?fallback_name current =
    match current with
    | JObject fields -> (
        match field "function" fields with
        | Some function_object -> from_json ?fallback_name function_object
        | None ->
            let explicit_name = first_string_field [ "name" ] fields in
            let input_schema =
              first_schema_field
                [ "inputSchema"; "input_schema"; "parameters"; "schema"; "input" ]
                fields
            in
            let output_schema =
              first_schema_field
                [
                  "outputSchema";
                  "output_schema";
                  "resultSchema";
                  "responseSchema";
                  "returns";
                ]
                fields
            in
            let description =
              first_string_field [ "description"; "summary" ] fields
            in
            begin
              match explicit_name, fallback_name with
              | Some name, _ ->
                  make_tool name description input_schema output_schema current
              | None, Some name ->
                  if input_schema <> None || output_schema <> None || description <> None then
                    make_tool name description input_schema output_schema current
                  else if looks_like_schema_object fields then
                    make_tool name None (Some current) None current
                  else
                    raise
                      (Manifest_error
                         (Printf.sprintf
                            "tool entry %s does not contain a name or schema fields"
                            name))
              | None, None ->
                  raise
                    (Manifest_error
                       "tool entry does not contain a name field")
            end)
    | _ -> (
        match fallback_name with
        | Some name -> make_tool name None (Some current) None current
        | None ->
            raise
              (Manifest_error
                 "tool entry must be an object when no fallback tool name exists"))
  in
  from_json ?fallback_name json

let parse_manifest_format value =
  match String.lowercase_ascii value with
  | "auto" -> Auto
  | "mcp-tools" -> Mcp_tools
  | "openai-tools" -> Openai_tools
  | "tool-map" -> Tool_map
  | "single-tool" -> Single_tool
  | other ->
      raise
        (Cli_error
           ("unsupported --manifest-format value: " ^ other))

let add_tool map tool =
  if StringMap.mem tool.name map then
    raise
      (Manifest_error
         (Printf.sprintf "duplicate tool name %s in manifest" tool.name))
  else
    StringMap.add tool.name tool map

let parse_tool_array format_name items =
  let tools =
    list_fold_lefti
      (fun index acc item ->
        let tool =
          try build_tool_spec item with
          | Manifest_error message ->
              raise
                (Manifest_error
                   (Printf.sprintf "%s tool[%d]: %s" format_name index message))
        in
        add_tool acc tool)
      StringMap.empty
      items
  in
  { format_name; tools }

let parse_tool_map fields =
  let tools =
    List.fold_left
      (fun acc (name, item) ->
        let tool =
          try build_tool_spec ~fallback_name:name item with
          | Manifest_error message ->
              raise
                (Manifest_error
                   (Printf.sprintf "tool-map entry %s: %s" name message))
        in
        add_tool acc tool)
      StringMap.empty
      fields
  in
  { format_name = "tool-map"; tools }

let parse_manifest format_hint document =
  let parse_single_tool current =
    let tool = build_tool_spec current in
    { format_name = "single-tool"; tools = add_tool StringMap.empty tool }
  in
  match format_hint, document with
  | Single_tool, _ -> parse_single_tool document
  | Mcp_tools, JObject fields -> (
      match first_field [ "tools" ] fields with
      | Some (JArray items) -> parse_tool_array "mcp-tools" items
      | Some _ -> raise (Manifest_error "mcp-tools format requires tools to be an array")
      | None -> raise (Manifest_error "mcp-tools format requires a top-level tools field"))
  | Openai_tools, JArray items -> parse_tool_array "openai-tools" items
  | Tool_map, JObject fields -> parse_tool_map fields
  | Auto, JObject fields -> (
      match first_field [ "tools" ] fields with
      | Some (JArray items) -> parse_tool_array "mcp-tools" items
      | Some _ -> raise (Manifest_error "top-level tools field must be an array")
      | None ->
          if first_field [ "name" ] fields <> None then
            parse_single_tool document
          else
            parse_tool_map fields)
  | Auto, JArray items -> parse_tool_array "openai-tools" items
  | Mcp_tools, _ ->
      raise (Manifest_error "mcp-tools format requires a top-level object")
  | Openai_tools, _ ->
      raise (Manifest_error "openai-tools format requires a top-level array")
  | Tool_map, _ ->
      raise (Manifest_error "tool-map format requires a top-level object")
  | Auto, _ ->
      raise
        (Manifest_error
           "auto-detection only supports a top-level array, tool-map object, or object with tools[]")

type type_set =
  | Any_type
  | Known_types of StringSet.t

let string_of_type_set = function
  | Any_type -> "any"
  | Known_types values -> string_of_string_set values

let schema_keywords_present fields names =
  List.exists (fun name -> field name fields <> None) names

let normalize_type_name value =
  let lowered = String.lowercase_ascii value in
  match lowered with
  | "string" | "number" | "integer" | "object" | "array" | "boolean" | "null" ->
      Some lowered
  | _ -> None

let infer_type_hints fields =
  let hints = ref StringSet.empty in
  if schema_keywords_present fields
       [ "properties"; "required"; "additionalProperties"; "minProperties"; "maxProperties" ]
  then
    hints := StringSet.add "object" !hints;
  if schema_keywords_present fields [ "items"; "minItems"; "maxItems"; "uniqueItems" ] then
    hints := StringSet.add "array" !hints;
  if schema_keywords_present fields [ "minLength"; "maxLength"; "pattern"; "format" ] then
    hints := StringSet.add "string" !hints;
  if schema_keywords_present fields
       [ "minimum"; "maximum"; "exclusiveMinimum"; "exclusiveMaximum"; "multipleOf" ]
  then
    hints := StringSet.add "number" !hints;
  if StringSet.is_empty !hints then None else Some !hints

let type_set_of_fields fields =
  let explicit_types =
    match field "type" fields with
    | Some (JString value) -> (
        match normalize_type_name value with
        | Some normalized -> Some (StringSet.singleton normalized)
        | None -> None)
    | Some (JArray items) ->
        List.fold_right
          (fun item acc ->
            match item, acc with
            | JString value, Some set -> (
                match normalize_type_name value with
                | Some normalized -> Some (StringSet.add normalized set)
                | None -> None)
            | _ -> None)
          items
          (Some StringSet.empty)
    | Some _ -> None
    | None -> infer_type_hints fields
  in
  match explicit_types with
  | None -> Any_type
  | Some types -> (
      match field "nullable" fields with
      | Some (JBool true) -> Known_types (StringSet.add "null" types)
      | _ -> Known_types types)

let type_allows accepted candidate =
  match accepted with
  | Any_type -> true
  | Known_types set ->
      StringSet.mem candidate set
      || (candidate = "integer" && StringSet.mem "number" set)

let uses_object_keywords fields =
  schema_keywords_present fields
    [ "properties"; "required"; "additionalProperties"; "minProperties"; "maxProperties" ]

let uses_array_keywords fields =
  schema_keywords_present fields [ "items"; "minItems"; "maxItems"; "uniqueItems" ]

let uses_string_keywords fields =
  schema_keywords_present fields [ "minLength"; "maxLength"; "pattern"; "format" ]

let uses_numeric_keywords fields =
  schema_keywords_present fields
    [ "minimum"; "maximum"; "exclusiveMinimum"; "exclusiveMaximum"; "multipleOf" ]

type validation_result =
  | Valid
  | Invalid of string
  | Unknown of string

let merge_validation left right =
  match left, right with
  | Invalid message, _ -> Invalid message
  | _, Invalid message -> Invalid message
  | Unknown message, _ -> Unknown message
  | _, Unknown message -> Unknown message
  | Valid, Valid -> Valid

let int_of_json = function
  | JNumber raw -> int_of_string_opt raw
  | _ -> None

let float_of_json = function
  | JNumber raw -> float_of_string_opt raw
  | _ -> None

let integer_literal value =
  match int_of_string_opt value with
  | Some _ -> true
  | None -> false

let json_sample_types = function
  | JNull -> StringSet.singleton "null"
  | JBool _ -> StringSet.singleton "boolean"
  | JString _ -> StringSet.singleton "string"
  | JObject _ -> StringSet.singleton "object"
  | JArray _ -> StringSet.singleton "array"
  | JNumber number ->
      if integer_literal number then string_set_of_list [ "integer"; "number" ]
      else StringSet.singleton "number"

let utf8_length value =
  let length = ref 0 in
  for index = 0 to String.length value - 1 do
    let byte = Char.code value.[index] in
    if byte land 0xc0 <> 0x80 then incr length
  done;
  !length

let present_unsupported_keys fields =
  List.fold_left
    (fun acc (name, _) ->
      if StringSet.mem name schema_combinator_keys then name :: acc else acc)
    []
    fields
  |> List.rev

type additional_properties =
  | Allow_any
  | Forbid
  | Allow_schema of json

let additional_properties_of_fields fields =
  match field "additionalProperties" fields with
  | None -> Allow_any
  | Some (JBool true) -> Allow_any
  | Some (JBool false) -> Forbid
  | Some schema -> Allow_schema schema

let properties_of_fields fields =
  match field "properties" fields with
  | Some (JObject properties) ->
      List.fold_left
        (fun acc (name, schema) -> StringMap.add name schema acc)
        StringMap.empty
        properties
  | _ -> StringMap.empty

let required_of_fields fields =
  match field "required" fields with
  | Some value -> (
      match string_list_of_json value with
      | Some values -> string_set_of_list values
      | None -> StringSet.empty)
  | None -> StringSet.empty

type numeric_bound = {
  value : float;
  exclusive : bool;
}

let lower_bound_of_fields fields =
  let from_minimum =
    match field "minimum" fields with
    | Some raw -> Option.map (fun value -> { value; exclusive = false }) (float_of_json raw)
    | None -> None
  in
  let from_exclusive =
    match field "exclusiveMinimum" fields with
    | Some raw -> Option.map (fun value -> { value; exclusive = true }) (float_of_json raw)
    | None -> None
  in
  match from_minimum, from_exclusive with
  | None, None -> None
  | Some bound, None | None, Some bound -> Some bound
  | Some minimum, Some exclusive ->
      if exclusive.value > minimum.value then Some exclusive
      else if exclusive.value < minimum.value then Some minimum
      else Some { value = minimum.value; exclusive = true }

let upper_bound_of_fields fields =
  let from_maximum =
    match field "maximum" fields with
    | Some raw -> Option.map (fun value -> { value; exclusive = false }) (float_of_json raw)
    | None -> None
  in
  let from_exclusive =
    match field "exclusiveMaximum" fields with
    | Some raw -> Option.map (fun value -> { value; exclusive = true }) (float_of_json raw)
    | None -> None
  in
  match from_maximum, from_exclusive with
  | None, None -> None
  | Some bound, None | None, Some bound -> Some bound
  | Some maximum, Some exclusive ->
      if exclusive.value < maximum.value then Some exclusive
      else if exclusive.value > maximum.value then Some maximum
      else Some { value = maximum.value; exclusive = true }

let validate_type_set fields value =
  match type_set_of_fields fields with
  | Any_type -> Valid
  | accepted ->
      let sample_types = json_sample_types value in
      if StringSet.exists (fun candidate -> type_allows accepted candidate) sample_types then
        Valid
      else
        Invalid
          (Printf.sprintf
             "value of type [%s] does not match schema type [%s]"
             (string_of_string_set sample_types)
             (string_of_type_set accepted))

let multiple_of_match lhs rhs =
  match lhs, rhs with
  | Some left, Some right when left > 0 && right > 0 -> left mod right = 0
  | _ -> false

let rec validate_sample root schema value =
  let resolved =
    try resolve_schema root schema with
    | Resolve_error message -> raise (Resolve_error message)
  in
  match resolved with
  | JBool true -> Valid
  | JBool false -> Invalid "schema is false"
  | JObject fields ->
      let unsupported = present_unsupported_keys fields in
      if unsupported <> [] then
        Unknown
          (Printf.sprintf
             "validation needs manual review because schema uses unsupported keywords: %s"
             (String.concat ", " unsupported))
      else
        let result = validate_type_set fields value in
        let result =
          match field "const" fields with
          | Some constant when not (json_equal constant value) ->
              Invalid "value does not match const"
          | _ -> result
        in
        let result =
          match field "enum" fields with
          | Some (JArray items) ->
              if List.exists (fun item -> json_equal item value) items then result
              else Invalid "value is not in enum"
          | Some _ -> Unknown "enum is not an array"
          | None -> result
        in
        match result with
        | Invalid _ | Unknown _ -> result
        | Valid -> (
            match value with
            | JObject object_fields ->
                let required = required_of_fields fields in
                let object_names = field_names object_fields in
                let missing_required =
                  StringSet.elements
                    (StringSet.diff required object_names)
                in
                if missing_required <> [] then
                  Invalid
                    (Printf.sprintf "missing required properties: %s"
                       (String.concat ", " missing_required))
                else
                  let property_constraints = properties_of_fields fields in
                  let property_count = List.length object_fields in
                  let result =
                    match field "minProperties" fields |> Option.bind int_of_json with
                    | Some minimum when property_count < minimum ->
                        Invalid
                          (Printf.sprintf
                             "object has %d properties but minimum is %d"
                             property_count
                             minimum)
                    | _ -> Valid
                  in
                  let result =
                    merge_validation result
                      (match field "maxProperties" fields |> Option.bind int_of_json with
                      | Some maximum when property_count > maximum ->
                          Invalid
                            (Printf.sprintf
                               "object has %d properties but maximum is %d"
                               property_count
                               maximum)
                      | _ -> Valid)
                  in
                  let result =
                    List.fold_left
                      (fun acc (name, property_value) ->
                        match acc with
                        | Invalid _ | Unknown _ -> acc
                        | Valid -> (
                            match StringMap.find_opt name property_constraints with
                            | Some property_schema ->
                                validate_sample root property_schema property_value
                            | None -> (
                                match additional_properties_of_fields fields with
                                | Allow_any -> Valid
                                | Forbid ->
                                    Invalid
                                      (Printf.sprintf
                                         "property %s is not allowed"
                                         name)
                                | Allow_schema schema ->
                                    validate_sample root schema property_value)))
                      result
                      object_fields
                  in
                  result
            | JArray items ->
                let item_count = List.length items in
                let result =
                  match field "minItems" fields |> Option.bind int_of_json with
                  | Some minimum when item_count < minimum ->
                      Invalid
                        (Printf.sprintf
                           "array has %d items but minimum is %d"
                           item_count
                           minimum)
                  | _ -> Valid
                in
                let result =
                  merge_validation result
                    (match field "maxItems" fields |> Option.bind int_of_json with
                    | Some maximum when item_count > maximum ->
                        Invalid
                          (Printf.sprintf
                             "array has %d items but maximum is %d"
                             item_count
                             maximum)
                    | _ -> Valid)
                in
                let result =
                  match field "uniqueItems" fields with
                  | Some (JBool true) ->
                      let rec has_duplicate remaining =
                        match remaining with
                        | [] -> false
                        | item :: rest ->
                            List.exists (fun candidate -> json_equal item candidate) rest
                            || has_duplicate rest
                      in
                      if has_duplicate items then Invalid "array violates uniqueItems"
                      else result
                  | _ -> result
                in
                let result =
                  match field "items" fields with
                  | Some item_schema ->
                      List.fold_left
                        (fun acc item ->
                          match acc with
                          | Invalid _ | Unknown _ -> acc
                          | Valid -> validate_sample root item_schema item)
                        result
                        items
                  | None -> result
                in
                result
            | JString string_value ->
                let length = utf8_length string_value in
                let result =
                  match field "minLength" fields |> Option.bind int_of_json with
                  | Some minimum when length < minimum ->
                      Invalid
                        (Printf.sprintf
                           "string length %d is below minimum %d"
                           length
                           minimum)
                  | _ -> Valid
                in
                let result =
                  merge_validation result
                    (match field "maxLength" fields |> Option.bind int_of_json with
                    | Some maximum when length > maximum ->
                        Invalid
                          (Printf.sprintf
                             "string length %d exceeds maximum %d"
                             length
                             maximum)
                    | _ -> Valid)
                in
                let result =
                  match field "pattern" fields with
                  | Some (JString _) -> Unknown "pattern validation is not implemented"
                  | Some _ -> Unknown "pattern is not a string"
                  | None -> result
                in
                let _ignored_format = field "format" fields in
                result
            | JNumber raw_number ->
                let numeric =
                  match float_of_string_opt raw_number with
                  | Some value -> value
                  | None -> nan
                in
                let result =
                  match lower_bound_of_fields fields with
                  | Some bound ->
                      if numeric > bound.value
                         || (numeric = bound.value && not bound.exclusive)
                      then Valid
                      else
                        Invalid
                          (Printf.sprintf
                             "number %s is below lower bound"
                             raw_number)
                  | None -> Valid
                in
                let result =
                  merge_validation result
                    (match upper_bound_of_fields fields with
                    | Some bound ->
                        if numeric < bound.value
                           || (numeric = bound.value && not bound.exclusive)
                        then Valid
                        else
                          Invalid
                            (Printf.sprintf
                               "number %s exceeds upper bound"
                               raw_number)
                    | None -> Valid)
                in
                let result =
                  match field "multipleOf" fields with
                  | Some (JNumber divisor) -> (
                      match float_of_string_opt divisor with
                      | Some factor when factor > 0.0 ->
                          let quotient = numeric /. factor in
                          if abs_float (quotient -. Float.round quotient) < 1e-9 then result
                          else Invalid "number does not satisfy multipleOf"
                      | _ -> Unknown "multipleOf is not a positive number")
                  | Some _ -> Unknown "multipleOf is not numeric"
                  | None -> result
                in
                result
            | JBool _ | JNull -> Valid)
  | _ -> Unknown "schema is not an object or boolean"

type relation_context = {
  tool_name : string;
  surface : string;
  lhs_root : json;
  rhs_root : json;
  unsupported_policy : unsupported_policy;
}

let push_issue issues severity ctx path code message =
  issues :=
    {
      severity;
      tool = Some ctx.tool_name;
      surface = ctx.surface;
      path;
      code;
      message;
    }
    :: !issues

let compare_lower_int_bound issues ctx path name lhs rhs =
  match lhs, rhs with
  | Some left, Some right when right > left ->
      push_issue issues Breaking ctx path (name ^ "-tightened")
        (Printf.sprintf "%s increased from %d to %d" name left right)
  | Some left, Some right when right < left ->
      push_issue issues Non_breaking ctx path (name ^ "-relaxed")
        (Printf.sprintf "%s decreased from %d to %d" name left right)
  | None, Some right ->
      push_issue issues Breaking ctx path (name ^ "-added")
        (Printf.sprintf "%s was introduced with value %d" name right)
  | Some left, None ->
      push_issue issues Non_breaking ctx path (name ^ "-removed")
        (Printf.sprintf "%s was removed from %d" name left)
  | _ -> ()

let compare_upper_int_bound issues ctx path name lhs rhs =
  match lhs, rhs with
  | Some left, Some right when right < left ->
      push_issue issues Breaking ctx path (name ^ "-tightened")
        (Printf.sprintf "%s decreased from %d to %d" name left right)
  | Some left, Some right when right > left ->
      push_issue issues Non_breaking ctx path (name ^ "-relaxed")
        (Printf.sprintf "%s increased from %d to %d" name left right)
  | None, Some right ->
      push_issue issues Breaking ctx path (name ^ "-added")
        (Printf.sprintf "%s was introduced with value %d" name right)
  | Some left, None ->
      push_issue issues Non_breaking ctx path (name ^ "-removed")
        (Printf.sprintf "%s was removed from %d" name left)
  | _ -> ()

let compare_lower_numeric_bound issues ctx path lhs rhs =
  match lhs, rhs with
  | Some left, Some right ->
      if right.value > left.value
         || (right.value = left.value && right.exclusive && not left.exclusive)
      then
        push_issue issues Breaking ctx path "lower-bound-tightened"
          (Printf.sprintf
             "lower numeric bound tightened from %g%s to %g%s"
             left.value
             (if left.exclusive then " exclusive" else "")
             right.value
             (if right.exclusive then " exclusive" else ""))
      else if right.value < left.value
              || (right.value = left.value && left.exclusive && not right.exclusive)
      then
        push_issue issues Non_breaking ctx path "lower-bound-relaxed"
          (Printf.sprintf
             "lower numeric bound relaxed from %g%s to %g%s"
             left.value
             (if left.exclusive then " exclusive" else "")
             right.value
             (if right.exclusive then " exclusive" else ""))
  | None, Some right ->
      push_issue issues Breaking ctx path "lower-bound-added"
        (Printf.sprintf
           "lower numeric bound introduced at %g%s"
           right.value
           (if right.exclusive then " exclusive" else ""))
  | Some left, None ->
      push_issue issues Non_breaking ctx path "lower-bound-removed"
        (Printf.sprintf
           "lower numeric bound removed from %g%s"
           left.value
           (if left.exclusive then " exclusive" else ""))
  | None, None -> ()

let compare_upper_numeric_bound issues ctx path lhs rhs =
  match lhs, rhs with
  | Some left, Some right ->
      if right.value < left.value
         || (right.value = left.value && right.exclusive && not left.exclusive)
      then
        push_issue issues Breaking ctx path "upper-bound-tightened"
          (Printf.sprintf
             "upper numeric bound tightened from %g%s to %g%s"
             left.value
             (if left.exclusive then " exclusive" else "")
             right.value
             (if right.exclusive then " exclusive" else ""))
      else if right.value > left.value
              || (right.value = left.value && left.exclusive && not right.exclusive)
      then
        push_issue issues Non_breaking ctx path "upper-bound-relaxed"
          (Printf.sprintf
             "upper numeric bound relaxed from %g%s to %g%s"
             left.value
             (if left.exclusive then " exclusive" else "")
             right.value
             (if right.exclusive then " exclusive" else ""))
  | None, Some right ->
      push_issue issues Breaking ctx path "upper-bound-added"
        (Printf.sprintf
           "upper numeric bound introduced at %g%s"
           right.value
           (if right.exclusive then " exclusive" else ""))
  | Some left, None ->
      push_issue issues Non_breaking ctx path "upper-bound-removed"
        (Printf.sprintf
           "upper numeric bound removed from %g%s"
           left.value
           (if left.exclusive then " exclusive" else ""))
  | None, None -> ()

let compare_type_sets issues ctx path lhs rhs =
  match lhs, rhs with
  | Any_type, Any_type -> ()
  | Known_types left, Any_type ->
      push_issue issues Non_breaking ctx path "type-relaxed"
        (Printf.sprintf "type restriction widened from [%s] to any"
           (string_of_string_set left))
  | Any_type, Known_types right ->
      push_issue issues Breaking ctx path "type-tightened"
        (Printf.sprintf "type restriction tightened from any to [%s]"
           (string_of_string_set right))
  | Known_types left, Known_types right ->
      let missing =
        StringSet.elements
          (StringSet.filter (fun candidate -> not (type_allows rhs candidate)) left)
      in
      if missing <> [] then
        push_issue issues Breaking ctx path "type-removed"
          (Printf.sprintf
             "schema no longer accepts types [%s]; remaining types are [%s]"
             (String.concat ", " missing)
             (string_of_string_set right));
      let added =
        StringSet.elements
          (StringSet.filter (fun candidate -> not (type_allows lhs candidate)) right)
      in
      if added <> [] then
        push_issue issues Non_breaking ctx path "type-added"
          (Printf.sprintf
             "schema additionally accepts types [%s]"
             (String.concat ", " added))

let rec compare_schema_subset issues ctx path lhs rhs =
  let lhs =
    try resolve_schema ctx.lhs_root lhs with
    | Resolve_error message ->
        push_issue issues (unsupported_severity ctx.unsupported_policy) ctx path
          "resolve-left"
          ("could not resolve old schema reference: " ^ message);
        lhs
  in
  let rhs =
    try resolve_schema ctx.rhs_root rhs with
    | Resolve_error message ->
        push_issue issues (unsupported_severity ctx.unsupported_policy) ctx path
          "resolve-right"
          ("could not resolve new schema reference: " ^ message);
        rhs
  in
  if semantic_json_equal lhs rhs then ()
  else
    match lhs, rhs with
    | JBool false, _ -> ()
    | _, JBool true -> ()
    | JBool true, JBool false ->
        push_issue issues Breaking ctx path "boolean-schema"
          "schema changed from allowing any value to rejecting every value"
    | JBool true, _ ->
        push_issue issues Breaking ctx path "boolean-schema"
          "schema changed from allowing any value to a constrained schema"
    | _, JBool false ->
        push_issue issues Breaking ctx path "boolean-schema"
          "new schema rejects values that the old schema allowed"
    | JObject lhs_fields, JObject rhs_fields ->
        let unsupported =
          StringSet.elements
            (StringSet.filter
               (fun key ->
                 StringSet.mem key schema_combinator_keys
                 && not (json_equal
                           (match field key lhs_fields with Some value -> value | None -> JNull)
                           (match field key rhs_fields with Some value -> value | None -> JNull)))
               (StringSet.union (field_names lhs_fields) (field_names rhs_fields)))
        in
        if unsupported <> [] then
          push_issue issues (unsupported_severity ctx.unsupported_policy) ctx path
            "unsupported-keywords"
            (Printf.sprintf
               "compatibility changed in unsupported JSON Schema keywords: %s"
               (String.concat ", " unsupported))
        else (
          compare_type_sets issues ctx path
            (type_set_of_fields lhs_fields)
            (type_set_of_fields rhs_fields);
          compare_const_and_enum issues ctx path lhs_fields rhs_fields;
          compare_object_rules issues ctx path lhs_fields rhs_fields;
          compare_array_rules issues ctx path lhs_fields rhs_fields;
          compare_string_rules issues ctx path lhs_fields rhs_fields;
          compare_numeric_rules issues ctx path lhs_fields rhs_fields)
    | _ ->
        push_issue issues (unsupported_severity ctx.unsupported_policy) ctx path
          "shape-change"
          "schema structure changed in a way that requires manual review"

and compare_const_and_enum issues ctx path lhs_fields rhs_fields =
  let lhs_const = field "const" lhs_fields in
  let rhs_const = field "const" rhs_fields in
  let lhs_enum = field "enum" lhs_fields in
  let rhs_enum = field "enum" rhs_fields in
  let validate_sample_against_rhs sample =
    match validate_sample ctx.rhs_root (JObject rhs_fields) sample with
    | Valid -> None
    | Invalid message -> Some (Breaking, message)
    | Unknown message -> Some (unsupported_severity ctx.unsupported_policy, message)
  in
  begin
    match lhs_const with
    | Some sample -> (
        match validate_sample_against_rhs sample with
        | Some (severity, message) ->
            push_issue issues severity ctx path "const-compatibility"
              (Printf.sprintf
                 "old const value is not accepted by the new schema: %s"
                 message)
        | None -> ())
    | None -> ()
  end;
  begin
    match lhs_enum with
    | Some (JArray samples) ->
        List.iteri
          (fun index sample ->
            match validate_sample_against_rhs sample with
            | Some (severity, message) ->
                push_issue issues severity ctx (index_path (child_path path "enum") index)
                  "enum-compatibility"
                  (Printf.sprintf
                     "old enum value is not accepted by the new schema: %s"
                     message)
            | None -> ())
          samples
    | Some _ ->
        push_issue issues (unsupported_severity ctx.unsupported_policy) ctx path
          "enum-shape"
          "enum is not an array"
    | None -> ()
  end;
  begin
    match lhs_const, lhs_enum, rhs_const with
    | None, None, Some sample ->
        push_issue issues Breaking ctx path "const-added"
          (Printf.sprintf
             "schema was tightened to a single const value %s"
             (json_to_string sample))
    | _ -> ()
  end;
  begin
    match lhs_const, lhs_enum, rhs_enum with
    | None, None, None -> ()
    | None, None, Some (JArray samples) ->
        push_issue issues Breaking ctx path "enum-added"
          (Printf.sprintf
             "schema was tightened to enum values [%s]"
             (String.concat ", " (List.map json_to_string samples)))
    | None, None, Some _ ->
        push_issue issues (unsupported_severity ctx.unsupported_policy) ctx path
          "enum-shape"
          "new enum is not an array"
    | Some _, _, _ | _, Some _, _ -> ()
  end;
  begin
    match lhs_enum, rhs_enum with
    | Some (JArray left_values), Some (JArray right_values) ->
        let added =
          List.filter
            (fun candidate ->
              not (List.exists (fun existing -> json_equal candidate existing) left_values))
            right_values
        in
        if added <> [] then
          push_issue issues Non_breaking ctx path "enum-expanded"
            (Printf.sprintf
               "enum now additionally allows [%s]"
               (String.concat ", " (List.map json_to_string added)))
    | Some (JArray _), None ->
        push_issue issues Non_breaking ctx path "enum-removed"
          "enum restriction was removed"
    | Some _, Some _ | Some _, None | None, Some _ | None, None -> ()
  end;
  begin
    match lhs_const, rhs_const with
    | Some left, None ->
        push_issue issues Non_breaking ctx path "const-removed"
          (Printf.sprintf
             "const restriction %s was removed"
             (json_to_string left))
    | Some left, Some right when not (json_equal left right) ->
        push_issue issues Breaking ctx path "const-changed"
          (Printf.sprintf
             "const changed from %s to %s"
             (json_to_string left)
             (json_to_string right))
    | _ -> ()
  end

and compare_object_rules issues ctx path lhs_fields rhs_fields =
  let lhs_objectish = uses_object_keywords lhs_fields in
  let rhs_objectish = uses_object_keywords rhs_fields in
  if lhs_objectish || rhs_objectish then (
    let lhs_required = required_of_fields lhs_fields in
    let rhs_required = required_of_fields rhs_fields in
    let newly_required =
      StringSet.elements (StringSet.diff rhs_required lhs_required)
    in
    if newly_required <> [] then
      push_issue issues Breaking ctx (child_path path "required") "required-added"
        (Printf.sprintf
           "new required properties were introduced: %s"
           (String.concat ", " newly_required));
    let no_longer_required =
      StringSet.elements (StringSet.diff lhs_required rhs_required)
    in
    if no_longer_required <> [] then
      push_issue issues Non_breaking ctx (child_path path "required")
        "required-removed"
        (Printf.sprintf
           "properties stopped being required: %s"
           (String.concat ", " no_longer_required));
    compare_lower_int_bound issues ctx path "minProperties"
      (field "minProperties" lhs_fields |> Option.bind int_of_json)
      (field "minProperties" rhs_fields |> Option.bind int_of_json);
    compare_upper_int_bound issues ctx path "maxProperties"
      (field "maxProperties" lhs_fields |> Option.bind int_of_json)
      (field "maxProperties" rhs_fields |> Option.bind int_of_json);
    let lhs_properties = properties_of_fields lhs_fields in
    let rhs_properties = properties_of_fields rhs_fields in
    StringMap.iter
      (fun name lhs_property ->
        let property_path = child_path (child_path path "properties") name in
        match StringMap.find_opt name rhs_properties with
        | Some rhs_property ->
            compare_schema_subset issues ctx property_path lhs_property rhs_property
        | None -> (
            match additional_properties_of_fields rhs_fields with
            | Allow_any ->
                push_issue issues Note ctx property_path "property-undocumented"
                  (Printf.sprintf
                     "named property %s was removed from properties but additionalProperties still allows it"
                     name)
            | Forbid ->
                push_issue issues Breaking ctx property_path "property-removed"
                  (Printf.sprintf
                     "property %s was removed and additionalProperties is false"
                     name)
            | Allow_schema schema ->
                push_issue issues Note ctx property_path "property-moved-to-additional"
                  (Printf.sprintf
                     "property %s now falls back to additionalProperties; checking compatibility against the fallback schema"
                     name);
                compare_schema_subset issues ctx
                  (child_path property_path "additionalProperties")
                  lhs_property
                  schema))
      lhs_properties;
    StringMap.iter
      (fun name _ ->
        if not (StringMap.mem name lhs_properties) then
          if StringSet.mem name rhs_required then
            ()
          else
            push_issue issues Non_breaking ctx
              (child_path (child_path path "properties") name)
              "property-added"
              (Printf.sprintf "optional property %s was added" name))
      rhs_properties;
    compare_additional_properties issues ctx path
      (additional_properties_of_fields lhs_fields)
      (additional_properties_of_fields rhs_fields))

and compare_additional_properties issues ctx path lhs rhs =
  let target_path = child_path path "additionalProperties" in
  match lhs, rhs with
  | Allow_any, Allow_any | Forbid, Forbid -> ()
  | Allow_any, Forbid ->
      push_issue issues Breaking ctx target_path "additional-properties-tightened"
        "additionalProperties changed from allowing any extra field to false"
  | Allow_any, Allow_schema _ ->
      push_issue issues Breaking ctx target_path "additional-properties-tightened"
        "additionalProperties changed from allowing any extra field to a restricted schema"
  | Forbid, Allow_any ->
      push_issue issues Non_breaking ctx target_path "additional-properties-relaxed"
        "additionalProperties changed from false to allowing any extra field"
  | Forbid, Allow_schema _ ->
      push_issue issues Non_breaking ctx target_path "additional-properties-relaxed"
        "additionalProperties changed from false to a schema for extra fields"
  | Allow_schema _, Allow_any ->
      push_issue issues Non_breaking ctx target_path "additional-properties-relaxed"
        "additionalProperties schema was removed, so extra fields are less restricted"
  | Allow_schema _, Forbid ->
      push_issue issues Breaking ctx target_path "additional-properties-tightened"
        "additionalProperties schema was replaced with false"
  | Allow_schema lhs_schema, Allow_schema rhs_schema ->
      compare_schema_subset issues ctx target_path lhs_schema rhs_schema

and compare_array_rules issues ctx path lhs_fields rhs_fields =
  let lhs_arrayish = uses_array_keywords lhs_fields in
  let rhs_arrayish = uses_array_keywords rhs_fields in
  if lhs_arrayish || rhs_arrayish then (
    compare_lower_int_bound issues ctx path "minItems"
      (field "minItems" lhs_fields |> Option.bind int_of_json)
      (field "minItems" rhs_fields |> Option.bind int_of_json);
    compare_upper_int_bound issues ctx path "maxItems"
      (field "maxItems" lhs_fields |> Option.bind int_of_json)
      (field "maxItems" rhs_fields |> Option.bind int_of_json);
    begin
      match field "uniqueItems" lhs_fields |> Option.bind json_bool,
            field "uniqueItems" rhs_fields |> Option.bind json_bool with
      | Some false, Some true | None, Some true ->
          push_issue issues Breaking ctx (child_path path "uniqueItems")
            "unique-items-tightened"
            "uniqueItems was enabled"
      | Some true, Some false | Some true, None ->
          push_issue issues Non_breaking ctx (child_path path "uniqueItems")
            "unique-items-relaxed"
            "uniqueItems was disabled"
      | _ -> ()
    end;
    match field "items" lhs_fields, field "items" rhs_fields with
    | Some lhs_items, Some rhs_items ->
        compare_schema_subset issues ctx (child_path path "items") lhs_items rhs_items
    | None, Some _ ->
        push_issue issues Breaking ctx (child_path path "items") "items-added"
          "items schema was introduced for previously unconstrained array members"
    | Some _, None ->
        push_issue issues Non_breaking ctx (child_path path "items") "items-removed"
          "items schema was removed"
    | None, None -> ())

and compare_string_rules issues ctx path lhs_fields rhs_fields =
  let lhs_stringish = uses_string_keywords lhs_fields in
  let rhs_stringish = uses_string_keywords rhs_fields in
  if lhs_stringish || rhs_stringish then (
    compare_lower_int_bound issues ctx path "minLength"
      (field "minLength" lhs_fields |> Option.bind int_of_json)
      (field "minLength" rhs_fields |> Option.bind int_of_json);
    compare_upper_int_bound issues ctx path "maxLength"
      (field "maxLength" lhs_fields |> Option.bind int_of_json)
      (field "maxLength" rhs_fields |> Option.bind int_of_json);
    begin
      match field "pattern" lhs_fields, field "pattern" rhs_fields with
      | None, Some (JString pattern) ->
          push_issue issues Breaking ctx (child_path path "pattern") "pattern-added"
            (Printf.sprintf "string pattern restriction %S was introduced" pattern)
      | Some (JString pattern), None ->
          push_issue issues Non_breaking ctx (child_path path "pattern")
            "pattern-removed"
            (Printf.sprintf "string pattern restriction %S was removed" pattern)
      | Some (JString left), Some (JString right) when left <> right ->
          push_issue issues (unsupported_severity ctx.unsupported_policy) ctx
            (child_path path "pattern")
            "pattern-changed"
            (Printf.sprintf
               "string pattern changed from %S to %S and needs manual review"
               left
               right)
      | Some _, Some _ | Some _, None | None, Some _ | None, None -> ()
    end;
    begin
      match field "format" lhs_fields |> Option.bind json_string,
            field "format" rhs_fields |> Option.bind json_string with
      | Some left, Some right when left <> right ->
          push_issue issues Note ctx (child_path path "format") "format-changed"
            (Printf.sprintf "format changed from %s to %s" left right)
      | None, Some right ->
          push_issue issues Note ctx (child_path path "format") "format-added"
            (Printf.sprintf "format %s was added" right)
      | Some left, None ->
          push_issue issues Note ctx (child_path path "format") "format-removed"
            (Printf.sprintf "format %s was removed" left)
      | _ -> ()
    end)

and compare_numeric_rules issues ctx path lhs_fields rhs_fields =
  let lhs_numericish = uses_numeric_keywords lhs_fields in
  let rhs_numericish = uses_numeric_keywords rhs_fields in
  if lhs_numericish || rhs_numericish then (
    compare_lower_numeric_bound issues ctx path
      (lower_bound_of_fields lhs_fields)
      (lower_bound_of_fields rhs_fields);
    compare_upper_numeric_bound issues ctx path
      (upper_bound_of_fields lhs_fields)
      (upper_bound_of_fields rhs_fields);
    begin
      match field "multipleOf" lhs_fields, field "multipleOf" rhs_fields with
      | None, Some (JNumber divisor) ->
          push_issue issues Breaking ctx (child_path path "multipleOf")
            "multiple-of-added"
            (Printf.sprintf "multipleOf %s was introduced" divisor)
      | Some (JNumber divisor), None ->
          push_issue issues Non_breaking ctx (child_path path "multipleOf")
            "multiple-of-removed"
            (Printf.sprintf "multipleOf %s was removed" divisor)
      | Some (JNumber left), Some (JNumber right) when left <> right ->
          if multiple_of_match (int_of_string_opt left) (int_of_string_opt right) then
            push_issue issues Non_breaking ctx (child_path path "multipleOf")
              "multiple-of-relaxed"
              (Printf.sprintf
                 "multipleOf changed from %s to %s"
                 left
                 right)
          else
            push_issue issues (unsupported_severity ctx.unsupported_policy) ctx
              (child_path path "multipleOf")
              "multiple-of-changed"
              (Printf.sprintf
                 "multipleOf changed from %s to %s and needs manual review"
                 left
                 right)
      | Some _, Some _ | Some _, None | None, Some _ | None, None -> ()
    end)

type report = {
  status : string;
  recommended_bump : string;
  issues : issue list;
  before_tool_count : int;
  after_tool_count : int;
  before_format : string;
  after_format : string;
}

type cli_config = {
  before_path : string option;
  after_path : string option;
  manifest_format : manifest_format;
  unsupported_policy : unsupported_policy;
  json_output : bool;
  ignore_description : bool;
  compare_output_schema : bool;
}

let default_cli_config =
  {
    before_path = None;
    after_path = None;
    manifest_format = Auto;
    unsupported_policy = Fail;
    json_output = false;
    ignore_description = false;
    compare_output_schema = true;
  }

let usage_text =
  String.concat "\n"
    [
      "Usage: McpManifestCompatGate.ml --before OLD.json --after NEW.json [options]";
      "";
      "Options:";
      "  --manifest-format auto|mcp-tools|openai-tools|tool-map|single-tool";
      "  --unsupported-policy fail|warn|ignore";
      "  --json";
      "  --ignore-description";
      "  --skip-output-schema";
      "  --help";
    ]

let parse_unsupported_policy value =
  match String.lowercase_ascii value with
  | "fail" -> Fail
  | "warn" -> Warn
  | "ignore" -> Ignore
  | other ->
      raise
        (Cli_error
           ("unsupported --unsupported-policy value: " ^ other))

let require_next argv index flag =
  if index + 1 >= Array.length argv then
    raise (Cli_error ("missing value for " ^ flag))
  else
    argv.(index + 1)

let parse_cli argv =
  let rec loop config index =
    if index >= Array.length argv then config
    else
      match argv.(index) with
      | "--before" ->
          let value = require_next argv index "--before" in
          loop { config with before_path = Some value } (index + 2)
      | "--after" ->
          let value = require_next argv index "--after" in
          loop { config with after_path = Some value } (index + 2)
      | "--manifest-format" ->
          let value = require_next argv index "--manifest-format" in
          loop { config with manifest_format = parse_manifest_format value } (index + 2)
      | "--unsupported-policy" ->
          let value = require_next argv index "--unsupported-policy" in
          loop { config with unsupported_policy = parse_unsupported_policy value } (index + 2)
      | "--json" ->
          loop { config with json_output = true } (index + 1)
      | "--ignore-description" ->
          loop { config with ignore_description = true } (index + 1)
      | "--skip-output-schema" ->
          loop { config with compare_output_schema = false } (index + 1)
      | "--help" | "-h" -> raise Help_requested
      | flag ->
          raise (Cli_error ("unknown argument: " ^ flag))
  in
  let config = loop default_cli_config 1 in
  match config.before_path, config.after_path with
  | Some _, Some _ -> config
  | _ -> raise (Cli_error "both --before and --after are required")

let compare_tool issues config before_tool after_tool =
  if not config.ignore_description && before_tool.description <> after_tool.description then
    let old_description =
      match before_tool.description with
      | Some value -> value
      | None -> "<none>"
    in
    let new_description =
      match after_tool.description with
      | Some value -> value
      | None -> "<none>"
    in
    issues :=
      {
        severity = Note;
        tool = Some before_tool.name;
        surface = "tool";
        path = root_path;
        code = "description-changed";
        message =
          Printf.sprintf "description changed from %S to %S"
            old_description
            new_description;
      }
      :: !issues;
  begin
    match before_tool.input_schema, after_tool.input_schema with
    | Some before_schema, Some after_schema ->
        compare_schema_subset issues
          {
            tool_name = before_tool.name;
            surface = "input";
            lhs_root = before_schema;
            rhs_root = after_schema;
            unsupported_policy = config.unsupported_policy;
          }
          root_path
          before_schema
          after_schema
    | None, Some _ ->
        issues :=
          {
            severity = Breaking;
            tool = Some before_tool.name;
            surface = "input";
            path = root_path;
            code = "input-schema-added";
            message =
              "input schema was introduced where the old tool accepted unspecified input";
          }
          :: !issues
    | Some _, None ->
        issues :=
          {
            severity = Non_breaking;
            tool = Some before_tool.name;
            surface = "input";
            path = root_path;
            code = "input-schema-removed";
            message = "input schema was removed";
          }
          :: !issues
    | None, None -> ()
  end;
  if config.compare_output_schema then
    match before_tool.output_schema, after_tool.output_schema with
    | Some before_schema, Some after_schema ->
        compare_schema_subset issues
          {
            tool_name = before_tool.name;
            surface = "output";
            lhs_root = after_schema;
            rhs_root = before_schema;
            unsupported_policy = config.unsupported_policy;
          }
          root_path
          after_schema
          before_schema
    | Some _, None | None, Some _ ->
        issues :=
          {
            severity = Note;
            tool = Some before_tool.name;
            surface = "output";
            path = root_path;
            code = "output-schema-presence-changed";
            message =
              "output schema exists on only one side, so strict output compatibility was skipped";
          }
          :: !issues
    | None, None -> ()

let compare_manifests config before after =
  let issues = ref [] in
  StringMap.iter
    (fun name before_tool ->
      match StringMap.find_opt name after.tools with
      | None ->
          issues :=
            {
              severity = Breaking;
              tool = Some name;
              surface = "manifest";
              path = root_path;
              code = "tool-removed";
              message = "tool was removed from the manifest";
            }
            :: !issues
      | Some after_tool ->
          compare_tool issues config before_tool after_tool)
    before.tools;
  StringMap.iter
    (fun name _ ->
      if not (StringMap.mem name before.tools) then
        issues :=
          {
            severity = Non_breaking;
            tool = Some name;
            surface = "manifest";
            path = root_path;
            code = "tool-added";
            message = "tool was added to the manifest";
          }
          :: !issues)
    after.tools;
  let issues =
    List.sort
      (fun left right ->
        let severity_order = compare (severity_rank left.severity) (severity_rank right.severity) in
        if severity_order <> 0 then severity_order
        else
          let tool_order = compare left.tool right.tool in
          if tool_order <> 0 then tool_order
          else
            let surface_order = String.compare left.surface right.surface in
            if surface_order <> 0 then surface_order
            else
              let path_order = String.compare left.path right.path in
              if path_order <> 0 then path_order else String.compare left.code right.code)
      !issues
  in
  let has_breaking =
    List.exists (fun issue -> issue.severity = Breaking) issues
  in
  let has_warning =
    List.exists (fun issue -> issue.severity = Warning) issues
  in
  let has_non_breaking =
    List.exists (fun issue -> issue.severity = Non_breaking) issues
  in
  let has_notes =
    List.exists (fun issue -> issue.severity = Note) issues
  in
  let status, recommended_bump =
    if has_breaking then ("breaking", "major")
    else if has_warning then ("needs-review", "review")
    else if has_non_breaking then ("compatible", "minor")
    else if has_notes then ("compatible", "patch")
    else ("compatible", "none")
  in
  {
    status;
    recommended_bump;
    issues;
    before_tool_count = StringMap.cardinal before.tools;
    after_tool_count = StringMap.cardinal after.tools;
    before_format = before.format_name;
    after_format = after.format_name;
  }

let issue_to_json issue =
  JObject
    [
      ("severity", JString (severity_name issue.severity));
      ( "tool",
        match issue.tool with
        | Some value -> JString value
        | None -> JNull );
      ("surface", JString issue.surface);
      ("path", JString issue.path);
      ("code", JString issue.code);
      ("message", JString issue.message);
    ]

let report_to_json report =
  JObject
    [
      ("status", JString report.status);
      ("recommendedBump", JString report.recommended_bump);
      ("beforeToolCount", JNumber (string_of_int report.before_tool_count));
      ("afterToolCount", JNumber (string_of_int report.after_tool_count));
      ("beforeFormat", JString report.before_format);
      ("afterFormat", JString report.after_format);
      ("issues", JArray (List.map issue_to_json report.issues));
    ]

let render_text_report report =
  let buffer = Buffer.create 1024 in
  let add_line text =
    Buffer.add_string buffer text;
    Buffer.add_char buffer '\n'
  in
  add_line "MCP Manifest Compatibility Gate";
  add_line
    (Printf.sprintf "Status: %s" report.status);
  add_line
    (Printf.sprintf "Recommended version bump: %s" report.recommended_bump);
  add_line
    (Printf.sprintf "Before: %d tools (%s)"
       report.before_tool_count
       report.before_format);
  add_line
    (Printf.sprintf "After: %d tools (%s)"
       report.after_tool_count
       report.after_format);
  add_line
    "Rule: new input contracts must accept all old inputs; new output contracts, when both sides provide them, must still satisfy old consumers.";
  if report.issues = [] then
    add_line "No compatibility issues detected."
  else
    List.iter
      (fun issue ->
        let scope =
          match issue.tool with
          | Some tool -> Printf.sprintf "tool=%s surface=%s" tool issue.surface
          | None -> Printf.sprintf "surface=%s" issue.surface
        in
        add_line
          (Printf.sprintf
             "[%s] %s path=%s code=%s %s"
             (String.uppercase_ascii (severity_name issue.severity))
             scope
             issue.path
             issue.code
             issue.message))
      report.issues;
  Buffer.contents buffer

let exit_code_for_report report =
  match report.status with
  | "breaking" -> 2
  | _ -> 0

let run () =
  let config = parse_cli Sys.argv in
  let before_path =
    match config.before_path with
    | Some value -> value
    | None -> assert false
  in
  let after_path =
    match config.after_path with
    | Some value -> value
    | None -> assert false
  in
  let before_document = parse_json_file before_path in
  let after_document = parse_json_file after_path in
  let before_manifest = parse_manifest config.manifest_format before_document in
  let after_manifest = parse_manifest config.manifest_format after_document in
  let report = compare_manifests config before_manifest after_manifest in
  if config.json_output then
    print_endline (json_to_string (report_to_json report))
  else
    print_string (render_text_report report);
  exit (exit_code_for_report report)

let () =
  try run () with
  | Help_requested ->
      print_endline usage_text;
      exit 0
  | Cli_error message ->
      prerr_endline message;
      prerr_endline usage_text;
      exit 64
  | Manifest_error message ->
      prerr_endline ("Manifest error: " ^ message);
      exit 65
  | Resolve_error message ->
      prerr_endline ("Schema resolution error: " ^ message);
      exit 66
  | Json_parse_error (location, message) ->
      prerr_endline
        (Printf.sprintf
           "JSON parse error at %d:%d: %s"
           location.line
           location.column
           message);
      exit 65

(*
This solves MCP tool manifest compatibility checks for teams that ship AI tools, agent runtimes, model gateways, and JSON Schema based contracts. Built because a very common April 2026 failure mode is changing a tool schema in a small way, merging it, and then finding out that older agents or cached clients can no longer call the tool. Use it when you need a CI gate for MCP manifests, OpenAI style tool arrays, or a simple tool-name-to-schema map and you want a clear answer about breaking versus non-breaking contract drift.

The trick: it treats input and output in the right direction. For inputs, the new schema has to keep accepting everything that old callers could already send. For outputs, when both sides publish output schemas, the new output still has to fit what old consumers expect. It also resolves local JSON Schema refs, checks object, array, string, enum, const, and numeric constraints, and falls back to review warnings when a schema starts using advanced combinators that need a human pass.

Drop this into a repo where you keep tool manifests or generated schema snapshots, run it in CI before deploys, and use the exit code to stop accidental breaking changes. I wrote it in a plain single-file OCaml style on purpose so it is easy to audit, easy to fork, and easy to find from searches like MCP compatibility gate, JSON Schema breaking change checker, MCP manifest semver checker, or OCaml tool contract validator.
*)
