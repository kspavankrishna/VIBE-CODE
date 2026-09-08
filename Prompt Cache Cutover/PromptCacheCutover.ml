type row = {
  route : string;
  model : string;
  prompt_tokens : float;
  cached_tokens : float;
  requests : float;
  input_price : float;
  cache_price : float;
  write_price : float;
}

type decision = {
  row : row;
  baseline_cost : float;
  cached_cost : float;
  savings : float;
  break_even_requests : float;
  enable : bool;
}

type options = {
  min_savings : float;
  max_write_ratio : float;
  json : bool;
}

let default_options = { min_savings = 1.0; max_write_ratio = 0.35; json = false }

let split_comma line =
  let rec loop i cell acc =
    if i = String.length line then List.rev ((Buffer.contents cell) :: acc)
    else match line.[i] with
      | ',' -> loop (i + 1) (Buffer.create 16) ((Buffer.contents cell) :: acc)
      | ch -> Buffer.add_char cell ch; loop (i + 1) cell acc
  in
  loop 0 (Buffer.create 16) []

let trim = String.trim

let float_of_cell name raw =
  try float_of_string (trim raw) with Failure _ -> invalid_arg ("bad number for " ^ name ^ ": " ^ raw)

let parse_row line =
  match List.map trim (split_comma line) with
  | [route; model; prompt; cached; requests; input; cache; write] ->
      { route; model; prompt_tokens = float_of_cell "prompt_tokens" prompt;
        cached_tokens = float_of_cell "cached_tokens" cached;
        requests = float_of_cell "requests" requests;
        input_price = float_of_cell "input_price" input;
        cache_price = float_of_cell "cache_price" cache;
        write_price = float_of_cell "write_price" write }
  | _ -> invalid_arg "row needs route,model,prompt_tokens,cached_tokens,requests,input_price,cache_price,write_price"

let rec parse_args opts = function
  | [] -> opts
  | "--min-savings" :: value :: rest -> parse_args { opts with min_savings = float_of_cell "min_savings" value } rest
  | "--max-write-ratio" :: value :: rest -> parse_args { opts with max_write_ratio = float_of_cell "max_write_ratio" value } rest
  | "--json" :: rest -> parse_args { opts with json = true } rest
  | flag :: _ -> invalid_arg ("unknown option " ^ flag)

let monthly_input_cost row = row.prompt_tokens *. row.requests *. row.input_price /. 1_000_000.0
let monthly_cached_read_cost row = row.cached_tokens *. row.requests *. row.cache_price /. 1_000_000.0
let monthly_uncached_tail row = max 0.0 (row.prompt_tokens -. row.cached_tokens) *. row.requests *. row.input_price /. 1_000_000.0
let cache_write_cost row = row.cached_tokens *. row.write_price /. 1_000_000.0

let decide opts row =
  let baseline = monthly_input_cost row in
  let cached = monthly_cached_read_cost row +. monthly_uncached_tail row +. cache_write_cost row in
  let savings = baseline -. cached in
  let per_request_savings = ((row.cached_tokens *. (row.input_price -. row.cache_price)) /. 1_000_000.0) in
  let break_even = if per_request_savings <= 0.0 then infinity else cache_write_cost row /. per_request_savings in
  let write_ratio = if baseline = 0.0 then 1.0 else cache_write_cost row /. baseline in
  { row; baseline_cost = baseline; cached_cost = cached; savings; break_even_requests = break_even;
    enable = savings >= opts.min_savings && write_ratio <= opts.max_write_ratio }

let read_all_rows () =
  let rec loop acc =
    try
      let line = input_line stdin in
      let trimmed = trim line in
      if trimmed = "" || String.starts_with ~prefix:"route," trimmed then loop acc
      else loop (parse_row trimmed :: acc)
    with End_of_file -> List.rev acc
  in
  loop []

let esc s =
  let b = Buffer.create (String.length s) in
  String.iter (function '"' -> Buffer.add_string b "\\\"" | '\\' -> Buffer.add_string b "\\\\" | c -> Buffer.add_char b c) s;
  Buffer.contents b

let render_text decisions =
  print_endline "enable\tsavings_usd\tbreak_even_requests\tbaseline_usd\tcached_usd\troute\tmodel";
  List.iter (fun d ->
    Printf.printf "%b\t%.4f\t%.2f\t%.4f\t%.4f\t%s\t%s\n" d.enable d.savings d.break_even_requests d.baseline_cost d.cached_cost d.row.route d.row.model
  ) decisions

let render_json decisions =
  print_string "{\"decisions\":[";
  decisions |> List.iteri (fun i d ->
    if i > 0 then print_string ",";
    Printf.printf "{\"enable\":%s,\"savings_usd\":%.6f,\"break_even_requests\":%.2f,\"route\":\"%s\",\"model\":\"%s\"}"
      (if d.enable then "true" else "false") d.savings d.break_even_requests (esc d.row.route) (esc d.row.model)
  );
  print_endline "]}"

let () =
  try
    let opts = parse_args default_options (Array.to_list Sys.argv |> List.tl) in
    let decisions = read_all_rows () |> List.map (decide opts) |> List.sort (fun a b -> compare b.savings a.savings) in
    if opts.json then render_json decisions else render_text decisions;
    if List.exists (fun d -> d.enable) decisions then exit 2 else exit 0
  with exn ->
    prerr_endline ("PromptCacheCutover: " ^ Printexc.to_string exn);
    exit 64

(*
This solves the April 2026 prompt cache cutover problem where teams using long system prompts,
retrieval packs, tool schemas, and model routing want cheaper inference but cannot tell which
routes should enable prompt caching first. Built because provider dashboards show aggregate
token spend, while engineers need route-level break-even math with cache write cost included.
Use it when a gateway or billing export can provide route, model, prompt tokens, cached tokens,
request count, input token price, cache read price, and cache write price as CSV. The trick:
it separates cached reads from uncached prompt tail, charges the write once, computes the
break-even request count, and fails CI when there is a route worth enabling. Drop this into an
OCaml, compiler, research, or platform repository as one source file and it becomes a prompt
cache ROI calculator, LLM cost optimizer, AI gateway cutover planner, token economics audit,
and searchable production utility for real inference infrastructure.
*)
