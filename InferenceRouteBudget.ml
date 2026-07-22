(* InferenceRouteBudget.ml
   Standalone OCaml CLI for auditing AI inference gateway routes before a
   rollout pushes cost, latency, carbon, or data residency past the budget. *)

type output_format =
  | Text
  | Json
  | Csv

type severity =
  | Info
  | Low
  | Medium
  | High
  | Critical

type config = {
  input_path : string option;
  output_format : output_format;
  monthly_requests_override : float option;
  max_p95_ms : float;
  max_p99_ms : float;
  max_error_rate : float;
  max_timeout_rate : float;
  max_monthly_cost_usd : float option;
  max_monthly_carbon_kg : float option;
  min_cache_hit_rate : float;
  min_headroom : float;
  cache_input_discount : float;
  require_residency : bool;
  fail_on : severity;
  top : int option;
  show_example : bool;
}

type route = {
  source_line : int;
  route : string;
  provider : string;
  model : string;
  region : string;
  jurisdiction : string;
  required_jurisdiction : string;
  data_class : string;
  rps : float;
  p95_ms : float;
  p99_ms : float;
  error_rate : float;
  timeout_rate : float;
  cost_usd_per_1k_input : float;
  cost_usd_per_1k_output : float;
  input_tokens_per_req : float;
  output_tokens_per_req : float;
  cache_hit_rate : float;
  renewable_pct : float;
  grid_gco2_per_kwh : float;
  kwh_per_1k_req : float;
  capacity_rps : float;
  criticality : float;
}

type finding = {
  severity : severity;
  code : string;
  message : string;
  advice : string;
}

type scored_route = {
  route_data : route;
  monthly_requests : float;
  gross_input_cost_usd : float;
  gross_output_cost_usd : float;
  cache_saved_usd : float;
  monthly_cost_usd : float;
  monthly_carbon_kg : float;
  headroom : float;
  score : float;
  decision : string;
  findings : finding list;
}

exception Config_error of string
exception Input_error of string

let seconds_per_month = 30.4375 *. 24.0 *. 60.0 *. 60.0

let default_config = {
  input_path = None;
  output_format = Text;
  monthly_requests_override = None;
  max_p95_ms = 900.0;
  max_p99_ms = 2500.0;
  max_error_rate = 0.01;
  max_timeout_rate = 0.005;
  max_monthly_cost_usd = None;
  max_monthly_carbon_kg = None;
  min_cache_hit_rate = 0.35;
  min_headroom = 0.20;
  cache_input_discount = 0.90;
  require_residency = true;
  fail_on = High;
  top = None;
  show_example = false;
}

let usage = "\
InferenceRouteBudget.ml audits AI inference gateway route exports.

Usage:
  ocaml InferenceRouteBudget.ml --input routes.csv [options]
  ./InferenceRouteBudget --input routes.csv --format json --fail-on high

Required CSV columns:
  route,provider,model,region,rps,p95_ms,p99_ms,error_rate,
  cost_usd_per_1k_input,cost_usd_per_1k_output,
  input_tokens_per_req,output_tokens_per_req

Useful optional CSV columns:
  timeout_rate,cache_hit_rate,renewable_pct,grid_gco2_per_kwh,
  kwh_per_1k_req,capacity_rps,jurisdiction,required_jurisdiction,
  data_class,criticality

Options:
  --input PATH                         Read CSV from PATH instead of stdin.
  --format text|json|csv               Output format. Default: text.
  --monthly-requests N                 Use one request volume for every row.
  --max-p95-ms N                       P95 latency budget. Default: 900.
  --max-p99-ms N                       P99 latency budget. Default: 2500.
  --max-error-rate R                   Fraction or percent. Default: 0.01.
  --max-timeout-rate R                 Fraction or percent. Default: 0.005.
  --max-monthly-cost-usd N             Per-route monthly cost budget.
  --max-monthly-carbon-kg N            Per-route monthly carbon budget.
  --min-cache-hit-rate R               Fraction or percent. Default: 0.35.
  --min-headroom R                     Required capacity headroom. Default: 0.20.
  --cache-input-discount R             Input-token cache discount. Default: 0.90.
  --require-residency true|false       Block jurisdiction mismatches. Default: true.
  --fail-on info|low|medium|high|critical
                                      Exit 2 when a finding reaches severity.
  --top N                              Print only the top N routes after sorting.
  --example                            Print an example CSV and exit.
  --help                               Show this help.
"

let trim = String.trim

let lowercase s = String.lowercase_ascii (trim s)

let severity_rank = function
  | Info -> 0
  | Low -> 1
  | Medium -> 2
  | High -> 3
  | Critical -> 4

let severity_to_string = function
  | Info -> "info"
  | Low -> "low"
  | Medium -> "medium"
  | High -> "high"
  | Critical -> "critical"

let severity_of_string value =
  match lowercase value with
  | "info" -> Info
  | "low" -> Low
  | "medium" | "med" -> Medium
  | "high" -> High
  | "critical" | "crit" | "blocker" -> Critical
  | other -> raise (Config_error ("unknown severity: " ^ other))

let format_of_string value =
  match lowercase value with
  | "text" | "human" -> Text
  | "json" -> Json
  | "csv" -> Csv
  | other -> raise (Config_error ("unknown output format: " ^ other))

let parse_bool field value =
  match lowercase value with
  | "1" | "true" | "t" | "yes" | "y" | "on" -> true
  | "0" | "false" | "f" | "no" | "n" | "off" -> false
  | _ -> raise (Config_error ("expected true or false for " ^ field ^ ", got: " ^ value))

let is_blank s = trim s = ""

let starts_with s prefix =
  let sl = String.length s and pl = String.length prefix in
  sl >= pl && String.sub s 0 pl = prefix

let ends_with s suffix =
  let sl = String.length s and tl = String.length suffix in
  sl >= tl && String.sub s (sl - tl) tl = suffix

let clamp lo hi value =
  if classify_float value = FP_nan then lo
  else if value < lo then lo
  else if value > hi then hi
  else value

let clean_number value =
  let b = Buffer.create (String.length value) in
  String.iter
    (fun c ->
       if c <> '_' && c <> ' ' then Buffer.add_char b c)
    value;
  Buffer.contents b

let parse_float_named field value =
  let v = trim value in
  if v = "" then raise (Input_error ("missing numeric value for " ^ field));
  try float_of_string (clean_number v) with
  | Failure _ -> raise (Input_error ("invalid numeric value for " ^ field ^ ": " ^ value))

let parse_nonnegative field value =
  let n = parse_float_named field value in
  if n < 0.0 then raise (Input_error (field ^ " must be non-negative, got: " ^ value));
  n

let parse_rate_fraction field value =
  let raw = trim value in
  if raw = "" then 0.0
  else
    let n =
      if ends_with raw "%" then
        parse_float_named field (String.sub raw 0 (String.length raw - 1)) /. 100.0
      else
        let x = parse_float_named field raw in
        if x > 1.0 then x /. 100.0 else x
    in
    if n < 0.0 || n > 1.0 then
      raise (Input_error (field ^ " must be between 0 and 1, or 0% and 100%"));
    n

let parse_percent field value =
  let raw = trim value in
  if raw = "" then 0.0
  else
    let n =
      if ends_with raw "%" then
        parse_float_named field (String.sub raw 0 (String.length raw - 1))
      else
        parse_float_named field raw
    in
    if n < 0.0 || n > 100.0 then
      raise (Input_error (field ^ " must be between 0 and 100"));
    n

let parse_optional_float field = function
  | None -> None
  | Some value -> Some (parse_nonnegative field value)

let require_arg argv i flag =
  if i + 1 >= Array.length argv then
    raise (Config_error ("missing value after " ^ flag));
  argv.(i + 1)

let parse_positive_int field value =
  try
    let n = int_of_string value in
    if n <= 0 then raise (Failure "not positive");
    n
  with Failure _ ->
    raise (Config_error (field ^ " must be a positive integer, got: " ^ value))

let parse_args () =
  let argv = Sys.argv in
  let rec loop i cfg =
    if i >= Array.length argv then cfg
    else
      match argv.(i) with
      | "--help" | "-h" ->
          print_string usage;
          exit 0
      | "--example" ->
          loop (i + 1) { cfg with show_example = true }
      | "--input" | "-i" ->
          let value = require_arg argv i argv.(i) in
          loop (i + 2) { cfg with input_path = Some value }
      | "--format" ->
          let value = require_arg argv i argv.(i) in
          loop (i + 2) { cfg with output_format = format_of_string value }
      | "--monthly-requests" ->
          let value = require_arg argv i argv.(i) in
          loop (i + 2) { cfg with monthly_requests_override = Some (parse_nonnegative "monthly_requests" value) }
      | "--max-p95-ms" ->
          let value = require_arg argv i argv.(i) in
          loop (i + 2) { cfg with max_p95_ms = parse_nonnegative "max_p95_ms" value }
      | "--max-p99-ms" ->
          let value = require_arg argv i argv.(i) in
          loop (i + 2) { cfg with max_p99_ms = parse_nonnegative "max_p99_ms" value }
      | "--max-error-rate" ->
          let value = require_arg argv i argv.(i) in
          loop (i + 2) { cfg with max_error_rate = parse_rate_fraction "max_error_rate" value }
      | "--max-timeout-rate" ->
          let value = require_arg argv i argv.(i) in
          loop (i + 2) { cfg with max_timeout_rate = parse_rate_fraction "max_timeout_rate" value }
      | "--max-monthly-cost-usd" ->
          let value = require_arg argv i argv.(i) in
          loop (i + 2) { cfg with max_monthly_cost_usd = parse_optional_float "max_monthly_cost_usd" (Some value) }
      | "--max-monthly-carbon-kg" ->
          let value = require_arg argv i argv.(i) in
          loop (i + 2) { cfg with max_monthly_carbon_kg = parse_optional_float "max_monthly_carbon_kg" (Some value) }
      | "--min-cache-hit-rate" ->
          let value = require_arg argv i argv.(i) in
          loop (i + 2) { cfg with min_cache_hit_rate = parse_rate_fraction "min_cache_hit_rate" value }
      | "--min-headroom" ->
          let value = require_arg argv i argv.(i) in
          loop (i + 2) { cfg with min_headroom = parse_rate_fraction "min_headroom" value }
      | "--cache-input-discount" ->
          let value = require_arg argv i argv.(i) in
          loop (i + 2) { cfg with cache_input_discount = parse_rate_fraction "cache_input_discount" value }
      | "--require-residency" ->
          let value = require_arg argv i argv.(i) in
          loop (i + 2) { cfg with require_residency = parse_bool "require_residency" value }
      | "--fail-on" ->
          let value = require_arg argv i argv.(i) in
          loop (i + 2) { cfg with fail_on = severity_of_string value }
      | "--top" ->
          let value = require_arg argv i argv.(i) in
          loop (i + 2) { cfg with top = Some (parse_positive_int "top" value) }
      | flag when starts_with flag "--" ->
          raise (Config_error ("unknown option: " ^ flag))
      | value ->
          raise (Config_error ("unexpected positional argument: " ^ value))
  in
  let cfg = loop 1 default_config in
  if cfg.max_p95_ms > cfg.max_p99_ms then
    raise (Config_error "--max-p95-ms cannot be larger than --max-p99-ms");
  cfg

let example_csv = "\
route,provider,model,region,jurisdiction,required_jurisdiction,data_class,rps,p95_ms,p99_ms,error_rate,timeout_rate,cost_usd_per_1k_input,cost_usd_per_1k_output,input_tokens_per_req,output_tokens_per_req,cache_hit_rate,renewable_pct,grid_gco2_per_kwh,kwh_per_1k_req,capacity_rps,criticality
primary-us,openai,gpt-5.2,us-east-1,US,US,internal,42,620,1380,0.004,0.001,0.012,0.08,1900,380,0.62,78,390,0.48,80,0.9
eu-safe,azure-openai,gpt-5.2-mini,westeurope,EU,EU,regulated,18,710,1710,0.006,0.002,0.010,0.06,1700,340,0.51,92,240,0.35,24,0.8
cheap-overflow,third-party,llama-4-large,ap-south-1,IN,US,pii,12,1120,3600,0.018,0.009,0.004,0.02,1600,310,0.18,44,610,0.55,13,0.6
"

let read_lines input_path =
  let channel, close_after =
    match input_path with
    | None -> stdin, false
    | Some path -> open_in path, true
  in
  let rec loop line_no acc =
    try
      let line = input_line channel in
      loop (line_no + 1) ((line_no, line) :: acc)
    with End_of_file ->
      if close_after then close_in channel;
      List.rev acc
  in
  try loop 1 [] with
  | exn ->
      if close_after then close_in_noerr channel;
      raise exn

let parse_csv_line line_no line =
  let len = String.length line in
  let fields = ref [] in
  let buf = Buffer.create 64 in
  let in_quotes = ref false in
  let i = ref 0 in
  let push () =
    fields := Buffer.contents buf :: !fields;
    Buffer.clear buf
  in
  while !i < len do
    let c = line.[!i] in
    if !in_quotes then begin
      if c = '"' then begin
        if !i + 1 < len && line.[!i + 1] = '"' then begin
          Buffer.add_char buf '"';
          i := !i + 2
        end else begin
          in_quotes := false;
          incr i
        end
      end else begin
        Buffer.add_char buf c;
        incr i
      end
    end else begin
      match c with
      | ',' ->
          push ();
          incr i
      | '"' ->
          if trim (Buffer.contents buf) <> "" then
            raise (Input_error (Printf.sprintf "line %d: quote must start a CSV field" line_no));
          in_quotes := true;
          incr i
      | _ ->
          Buffer.add_char buf c;
          incr i
    end
  done;
  if !in_quotes then
    raise (Input_error (Printf.sprintf "line %d: unterminated quoted CSV field" line_no));
  push ();
  List.rev !fields

let compact_header header =
  let b = Buffer.create (String.length header) in
  String.iter
    (fun c ->
       let c = Char.lowercase_ascii c in
       if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') then Buffer.add_char b c)
    header;
  Buffer.contents b

let canonical_header header =
  match compact_header header with
  | "route" | "routename" | "pool" | "gateway" -> Some "route"
  | "provider" | "vendor" | "platform" -> Some "provider"
  | "model" | "modelname" -> Some "model"
  | "region" | "cloudregion" -> Some "region"
  | "jurisdiction" | "datajurisdiction" | "actualjurisdiction" -> Some "jurisdiction"
  | "requiredjurisdiction" | "requireddatajurisdiction" | "residency" -> Some "required_jurisdiction"
  | "dataclass" | "classification" | "sensitivity" -> Some "data_class"
  | "rps" | "requestspersecond" | "qps" -> Some "rps"
  | "p95ms" | "latencyp95ms" | "p95latencyms" -> Some "p95_ms"
  | "p99ms" | "latencyp99ms" | "p99latencyms" -> Some "p99_ms"
  | "errorrate" | "errors" | "failedrate" -> Some "error_rate"
  | "timeoutrate" | "timeouts" -> Some "timeout_rate"
  | "costusdper1kinput" | "inputcostusdper1k" | "inputcost" -> Some "cost_usd_per_1k_input"
  | "costusdper1koutput" | "outputcostusdper1k" | "outputcost" -> Some "cost_usd_per_1k_output"
  | "inputtokensperreq" | "prompttokensperreq" | "avginputtokens" -> Some "input_tokens_per_req"
  | "outputtokensperreq" | "completiontokensperreq" | "avgoutputtokens" -> Some "output_tokens_per_req"
  | "cachehitrate" | "prompthit" | "prompthitrate" -> Some "cache_hit_rate"
  | "renewablepct" | "renewablepercent" | "renewableshare" -> Some "renewable_pct"
  | "gridgco2perkwh" | "carbonintensitygco2perkwh" | "carbonintensity" -> Some "grid_gco2_per_kwh"
  | "kwhper1kreq" | "energykwhper1krequests" | "energyper1krequests" -> Some "kwh_per_1k_req"
  | "capacityrps" | "maxrps" | "ratedrps" -> Some "capacity_rps"
  | "criticality" | "businesscriticality" -> Some "criticality"
  | "" -> None
  | other -> Some other

let build_header_table headers =
  let table = Hashtbl.create 32 in
  List.iteri
    (fun idx header ->
       match canonical_header header with
       | None -> ()
       | Some name ->
           if not (Hashtbl.mem table name) then Hashtbl.add table name idx)
    headers;
  table

let require_column table name =
  if not (Hashtbl.mem table name) then
    raise (Input_error ("missing required CSV column: " ^ name))

let get_field table row name =
  match Hashtbl.find_opt table name with
  | None -> ""
  | Some idx ->
      if idx >= Array.length row then "" else trim row.(idx)

let get_required_field line_no table row name =
  let value = get_field table row name in
  if value = "" then
    raise (Input_error (Printf.sprintf "line %d: missing required value for %s" line_no name));
  value

let parse_routes lines =
  let meaningful =
    List.filter
      (fun (_, line) ->
         let t = trim line in
         t <> "" && not (starts_with t "#"))
      lines
  in
  match meaningful with
  | [] -> raise (Input_error "input CSV is empty")
  | (header_line_no, header_line) :: data_lines ->
      let headers = parse_csv_line header_line_no header_line in
      let table = build_header_table headers in
      List.iter (require_column table)
        [ "route"; "provider"; "model"; "region"; "rps"; "p95_ms"; "p99_ms";
          "error_rate"; "cost_usd_per_1k_input"; "cost_usd_per_1k_output";
          "input_tokens_per_req"; "output_tokens_per_req" ];
      let parse_row (line_no, line) =
        let row = Array.of_list (parse_csv_line line_no line) in
        let required name = get_required_field line_no table row name in
        let optional name default =
          let value = get_field table row name in
          if value = "" then default else value
        in
        let route_name = required "route" in
        let region = required "region" in
        let rps = parse_nonnegative "rps" (required "rps") in
        let capacity = optional "capacity_rps" "" in
        {
          source_line = line_no;
          route = route_name;
          provider = required "provider";
          model = required "model";
          region;
          jurisdiction = optional "jurisdiction" region;
          required_jurisdiction = optional "required_jurisdiction" "";
          data_class = lowercase (optional "data_class" "internal");
          rps;
          p95_ms = parse_nonnegative "p95_ms" (required "p95_ms");
          p99_ms = parse_nonnegative "p99_ms" (required "p99_ms");
          error_rate = parse_rate_fraction "error_rate" (required "error_rate");
          timeout_rate = parse_rate_fraction "timeout_rate" (optional "timeout_rate" "0");
          cost_usd_per_1k_input =
            parse_nonnegative "cost_usd_per_1k_input" (required "cost_usd_per_1k_input");
          cost_usd_per_1k_output =
            parse_nonnegative "cost_usd_per_1k_output" (required "cost_usd_per_1k_output");
          input_tokens_per_req =
            parse_nonnegative "input_tokens_per_req" (required "input_tokens_per_req");
          output_tokens_per_req =
            parse_nonnegative "output_tokens_per_req" (required "output_tokens_per_req");
          cache_hit_rate = parse_rate_fraction "cache_hit_rate" (optional "cache_hit_rate" "0");
          renewable_pct = parse_percent "renewable_pct" (optional "renewable_pct" "0");
          grid_gco2_per_kwh =
            parse_nonnegative "grid_gco2_per_kwh" (optional "grid_gco2_per_kwh" "450");
          kwh_per_1k_req =
            parse_nonnegative "kwh_per_1k_req" (optional "kwh_per_1k_req" "0");
          capacity_rps =
            if capacity = "" then rps else parse_nonnegative "capacity_rps" capacity;
          criticality =
            clamp 0.0 1.0 (parse_rate_fraction "criticality" (optional "criticality" "0.5"));
        }
      in
      let routes = List.map parse_row data_lines in
      if routes = [] then raise (Input_error "input CSV has a header but no route rows");
      routes

let add_finding severity code message advice findings =
  { severity; code; message; advice } :: findings

let pct value = value *. 100.0

let ratio_over actual budget =
  if budget <= 0.0 then 0.0 else actual /. budget

let severity_for_ratio ratio medium high critical =
  if ratio >= critical then Critical
  else if ratio >= high then High
  else if ratio >= medium then Medium
  else Low

let jurisdiction_mismatch actual required =
  let actual = lowercase actual and required = lowercase required in
  required <> "" && actual <> required

let classify_data_class value =
  match lowercase value with
  | "pii" | "phi" | "pci" | "regulated" | "customer" | "personal" -> "regulated"
  | "public" -> "public"
  | _ -> "internal"

let score_route cfg route_data =
  let monthly_requests =
    match cfg.monthly_requests_override with
    | Some requests -> requests
    | None -> route_data.rps *. seconds_per_month
  in
  let gross_input_cost =
    monthly_requests *. route_data.input_tokens_per_req /. 1000.0
    *. route_data.cost_usd_per_1k_input
  in
  let gross_output_cost =
    monthly_requests *. route_data.output_tokens_per_req /. 1000.0
    *. route_data.cost_usd_per_1k_output
  in
  let cache_saved =
    gross_input_cost *. route_data.cache_hit_rate *. cfg.cache_input_discount
  in
  let monthly_cost = max 0.0 (gross_input_cost +. gross_output_cost -. cache_saved) in
  let renewable_factor = 1.0 -. (route_data.renewable_pct /. 100.0) in
  let monthly_carbon_kg =
    monthly_requests /. 1000.0 *. route_data.kwh_per_1k_req
    *. route_data.grid_gco2_per_kwh *. renewable_factor /. 1000.0
  in
  let headroom =
    if route_data.rps <= 0.0 then 1.0
    else (route_data.capacity_rps /. route_data.rps) -. 1.0
  in
  let findings = ref [] in
  if route_data.p95_ms > cfg.max_p95_ms then
    let ratio = ratio_over route_data.p95_ms cfg.max_p95_ms in
    let sev = severity_for_ratio ratio 1.0 1.25 1.75 in
    findings := add_finding sev "latency_p95"
      (Printf.sprintf "p95 %.0fms is over the %.0fms budget" route_data.p95_ms cfg.max_p95_ms)
      "Reduce prompt size, warm the route, move traffic closer to users, or keep this route out of the primary pool."
      !findings;
  if route_data.p99_ms > cfg.max_p99_ms then
    let ratio = ratio_over route_data.p99_ms cfg.max_p99_ms in
    let sev = severity_for_ratio ratio 1.0 1.20 1.50 in
    findings := add_finding sev "latency_p99"
      (Printf.sprintf "p99 %.0fms is over the %.0fms budget" route_data.p99_ms cfg.max_p99_ms)
      "Do not send interactive traffic here until tail latency has a bounded retry and fallback policy."
      !findings;
  if route_data.error_rate > cfg.max_error_rate then
    let ratio = ratio_over route_data.error_rate cfg.max_error_rate in
    let sev = severity_for_ratio ratio 1.0 1.5 3.0 in
    findings := add_finding sev "error_rate"
      (Printf.sprintf "error rate %.3f%% is over the %.3f%% budget"
         (pct route_data.error_rate) (pct cfg.max_error_rate))
      "Hold rollout, inspect provider status, and separate model errors from gateway errors before adding traffic."
      !findings;
  if route_data.timeout_rate > cfg.max_timeout_rate then
    let ratio = ratio_over route_data.timeout_rate cfg.max_timeout_rate in
    let sev = severity_for_ratio ratio 1.0 1.5 3.0 in
    findings := add_finding sev "timeout_rate"
      (Printf.sprintf "timeout rate %.3f%% is over the %.3f%% budget"
         (pct route_data.timeout_rate) (pct cfg.max_timeout_rate))
      "Treat timeouts as user-visible failures and require circuit breaking before this route receives more traffic."
      !findings;
  if route_data.cache_hit_rate < cfg.min_cache_hit_rate then
    findings := add_finding Medium "prompt_cache"
      (Printf.sprintf "prompt cache hit rate %.1f%% is below the %.1f%% target"
         (pct route_data.cache_hit_rate) (pct cfg.min_cache_hit_rate))
      "Normalize cache keys, isolate volatile context, and measure cached-token billing before raising volume."
      !findings;
  if headroom < cfg.min_headroom then
    let sev = if headroom < 0.0 then Critical else High in
    findings := add_finding sev "capacity_headroom"
      (Printf.sprintf "capacity headroom %.1f%% is below the %.1f%% target"
         (pct headroom) (pct cfg.min_headroom))
      "Reserve capacity or throttle traffic before peak load; this route has little room for retries or bursts."
      !findings;
  begin match cfg.max_monthly_cost_usd with
  | None -> ()
  | Some budget ->
      if monthly_cost > budget then
        let ratio = ratio_over monthly_cost budget in
        let sev = severity_for_ratio ratio 1.0 1.25 1.75 in
        findings := add_finding sev "monthly_cost"
          (Printf.sprintf "monthly cost $%.2f is over the $%.2f route budget" monthly_cost budget)
          "Lower token volume, raise cache hit rate, or move only latency-sensitive work to this route."
          !findings
  end;
  begin match cfg.max_monthly_carbon_kg with
  | None -> ()
  | Some budget ->
      if monthly_carbon_kg > budget then
        let ratio = ratio_over monthly_carbon_kg budget in
        let sev = severity_for_ratio ratio 1.0 1.5 2.5 in
        findings := add_finding sev "carbon_budget"
          (Printf.sprintf "monthly carbon %.2fkg CO2e is over the %.2fkg route budget"
             monthly_carbon_kg budget)
          "Prefer a lower-carbon region for batch work or delay flexible jobs until cleaner grid windows."
          !findings
  end;
  if cfg.require_residency
     && jurisdiction_mismatch route_data.jurisdiction route_data.required_jurisdiction then
    findings := add_finding Critical "data_residency"
      (Printf.sprintf "jurisdiction %s does not match required jurisdiction %s"
         route_data.jurisdiction route_data.required_jurisdiction)
      "Block regulated traffic until the route is pinned to the required jurisdiction and audited end to end."
      !findings;
  if classify_data_class route_data.data_class = "regulated"
     && trim route_data.required_jurisdiction = "" then
    findings := add_finding High "missing_residency_rule"
      "regulated data has no required_jurisdiction value"
      "Add an explicit residency requirement to the export so policy can fail closed instead of guessing."
      !findings;
  if route_data.p99_ms < route_data.p95_ms then
    findings := add_finding High "invalid_latency_shape"
      "p99 latency is lower than p95 latency"
      "Fix the upstream metric export before using this route in an automated rollout decision."
      !findings;
  let latency_penalty =
    18.0 *. max 0.0 ((route_data.p95_ms /. cfg.max_p95_ms) -. 1.0)
    +. 22.0 *. max 0.0 ((route_data.p99_ms /. cfg.max_p99_ms) -. 1.0)
  in
  let reliability_penalty =
    60.0 *. max 0.0 ((route_data.error_rate /. max cfg.max_error_rate 0.000001) -. 1.0)
    +. 40.0 *. max 0.0 ((route_data.timeout_rate /. max cfg.max_timeout_rate 0.000001) -. 1.0)
  in
  let cache_penalty =
    if route_data.cache_hit_rate >= cfg.min_cache_hit_rate then 0.0
    else 12.0 *. ((cfg.min_cache_hit_rate -. route_data.cache_hit_rate) /. max cfg.min_cache_hit_rate 0.000001)
  in
  let headroom_penalty =
    if headroom >= cfg.min_headroom then 0.0
    else 25.0 *. ((cfg.min_headroom -. headroom) /. max cfg.min_headroom 0.000001)
  in
  let residency_penalty =
    if cfg.require_residency
       && jurisdiction_mismatch route_data.jurisdiction route_data.required_jurisdiction
    then 100.0 else 0.0
  in
  let cost_penalty =
    match cfg.max_monthly_cost_usd with
    | None -> 0.0
    | Some budget -> 20.0 *. max 0.0 ((monthly_cost /. max budget 0.000001) -. 1.0)
  in
  let carbon_penalty =
    match cfg.max_monthly_carbon_kg with
    | None -> 0.0
    | Some budget -> 10.0 *. max 0.0 ((monthly_carbon_kg /. max budget 0.000001) -. 1.0)
  in
  let criticality_multiplier = 0.75 +. route_data.criticality *. 0.5 in
  let penalty =
    (latency_penalty +. reliability_penalty +. cache_penalty +. headroom_penalty
     +. residency_penalty +. cost_penalty +. carbon_penalty)
    *. criticality_multiplier
  in
  let score = clamp 0.0 100.0 (100.0 -. penalty) in
  let sorted_findings =
    List.sort
      (fun a b ->
         let by_severity = compare (severity_rank b.severity) (severity_rank a.severity) in
         if by_severity <> 0 then by_severity else compare a.code b.code)
      !findings
  in
  let max_severity =
    List.fold_left
      (fun acc f -> if severity_rank f.severity > severity_rank acc then f.severity else acc)
      Info sorted_findings
  in
  let decision =
    match max_severity with
    | Critical -> "block"
    | High -> "degrade"
    | Medium -> if score >= 75.0 then "watch" else "degrade"
    | Low | Info -> if score >= 90.0 then "preferred" else "watch"
  in
  {
    route_data;
    monthly_requests;
    gross_input_cost_usd = gross_input_cost;
    gross_output_cost_usd = gross_output_cost;
    cache_saved_usd = cache_saved;
    monthly_cost_usd = monthly_cost;
    monthly_carbon_kg;
    headroom;
    score;
    decision;
    findings = sorted_findings;
  }

let sort_scored scored =
  List.sort
    (fun a b ->
       let decision_rank d =
         match d with
         | "preferred" -> 0
         | "watch" -> 1
         | "degrade" -> 2
         | "block" -> 3
         | _ -> 4
       in
       let by_decision = compare (decision_rank a.decision) (decision_rank b.decision) in
       if by_decision <> 0 then by_decision
       else
         let by_score = compare b.score a.score in
         if by_score <> 0 then by_score
         else compare a.route_data.route b.route_data.route)
    scored

let take top rows =
  match top with
  | None -> rows
  | Some n ->
      let rec loop remaining acc = function
        | [] -> List.rev acc
        | _ when remaining <= 0 -> List.rev acc
        | x :: xs -> loop (remaining - 1) (x :: acc) xs
      in
      loop n [] rows

let max_finding_severity scored =
  List.fold_left
    (fun acc route ->
       List.fold_left
         (fun inner finding ->
            if severity_rank finding.severity > severity_rank inner then finding.severity else inner)
         acc route.findings)
    Info scored

let count_by_decision decision scored =
  List.fold_left
    (fun count route -> if route.decision = decision then count + 1 else count)
    0 scored

let sum_by f scored =
  List.fold_left (fun total route -> total +. f route) 0.0 scored

let json_escape s =
  let b = Buffer.create (String.length s + 8) in
  String.iter
    (function
      | '"' -> Buffer.add_string b "\\\""
      | '\\' -> Buffer.add_string b "\\\\"
      | '\b' -> Buffer.add_string b "\\b"
      | '\012' -> Buffer.add_string b "\\f"
      | '\n' -> Buffer.add_string b "\\n"
      | '\r' -> Buffer.add_string b "\\r"
      | '\t' -> Buffer.add_string b "\\t"
      | c when Char.code c < 32 ->
          Buffer.add_string b (Printf.sprintf "\\u%04x" (Char.code c))
      | c -> Buffer.add_char b c)
    s;
  Buffer.contents b

let json_string s = "\"" ^ json_escape s ^ "\""

let csv_escape s =
  if String.exists (fun c -> c = ',' || c = '"' || c = '\n' || c = '\r') s then
    let b = Buffer.create (String.length s + 8) in
    Buffer.add_char b '"';
    String.iter
      (fun c ->
         if c = '"' then Buffer.add_string b "\"\""
         else Buffer.add_char b c)
      s;
    Buffer.add_char b '"';
    Buffer.contents b
  else s

let primary_finding route =
  match route.findings with
  | [] -> None
  | f :: _ -> Some f

let emit_text scored =
  let total_cost = sum_by (fun r -> r.monthly_cost_usd) scored in
  let total_carbon = sum_by (fun r -> r.monthly_carbon_kg) scored in
  Printf.printf "Inference route budget audit\n";
  Printf.printf "routes=%d preferred=%d watch=%d degrade=%d block=%d monthly_cost=$%.2f monthly_carbon=%.2fkg\n\n"
    (List.length scored)
    (count_by_decision "preferred" scored)
    (count_by_decision "watch" scored)
    (count_by_decision "degrade" scored)
    (count_by_decision "block" scored)
    total_cost total_carbon;
  List.iter
    (fun sr ->
       let r = sr.route_data in
       Printf.printf "[%s] %s provider=%s model=%s region=%s score=%.1f cost=$%.2f carbon=%.2fkg p95=%.0fms p99=%.0fms errors=%.3f%% cache=%.1f%% headroom=%.1f%%\n"
         (String.uppercase_ascii sr.decision)
         r.route r.provider r.model r.region sr.score sr.monthly_cost_usd
         sr.monthly_carbon_kg r.p95_ms r.p99_ms (pct r.error_rate)
         (pct r.cache_hit_rate) (pct sr.headroom);
       begin match sr.findings with
       | [] -> Printf.printf "  ok: route is inside the configured budget envelope\n"
       | findings ->
           List.iter
             (fun f ->
                Printf.printf "  %s %s: %s\n      %s\n"
                  (String.uppercase_ascii (severity_to_string f.severity))
                  f.code f.message f.advice)
             findings
       end;
       print_newline ())
    scored

let emit_csv scored =
  print_endline
    "decision,score,route,provider,model,region,jurisdiction,required_jurisdiction,monthly_requests,monthly_cost_usd,cache_saved_usd,monthly_carbon_kg,p95_ms,p99_ms,error_rate,timeout_rate,cache_hit_rate,headroom,primary_severity,primary_code,primary_message";
  List.iter
    (fun sr ->
       let r = sr.route_data in
       let primary_severity, primary_code, primary_message =
         match primary_finding sr with
         | None -> "info", "ok", "route is inside the configured budget envelope"
         | Some f -> severity_to_string f.severity, f.code, f.message
       in
       let fields = [
         sr.decision;
         Printf.sprintf "%.1f" sr.score;
         r.route;
         r.provider;
         r.model;
         r.region;
         r.jurisdiction;
         r.required_jurisdiction;
         Printf.sprintf "%.0f" sr.monthly_requests;
         Printf.sprintf "%.2f" sr.monthly_cost_usd;
         Printf.sprintf "%.2f" sr.cache_saved_usd;
         Printf.sprintf "%.2f" sr.monthly_carbon_kg;
         Printf.sprintf "%.0f" r.p95_ms;
         Printf.sprintf "%.0f" r.p99_ms;
         Printf.sprintf "%.6f" r.error_rate;
         Printf.sprintf "%.6f" r.timeout_rate;
         Printf.sprintf "%.6f" r.cache_hit_rate;
         Printf.sprintf "%.6f" sr.headroom;
         primary_severity;
         primary_code;
         primary_message;
       ] in
       print_endline (String.concat "," (List.map csv_escape fields)))
    scored

let emit_json scored =
  let total_cost = sum_by (fun r -> r.monthly_cost_usd) scored in
  let total_carbon = sum_by (fun r -> r.monthly_carbon_kg) scored in
  print_endline "{";
  Printf.printf "  \"route_count\": %d,\n" (List.length scored);
  Printf.printf "  \"preferred_count\": %d,\n" (count_by_decision "preferred" scored);
  Printf.printf "  \"watch_count\": %d,\n" (count_by_decision "watch" scored);
  Printf.printf "  \"degrade_count\": %d,\n" (count_by_decision "degrade" scored);
  Printf.printf "  \"block_count\": %d,\n" (count_by_decision "block" scored);
  Printf.printf "  \"monthly_cost_usd\": %.6f,\n" total_cost;
  Printf.printf "  \"monthly_carbon_kg\": %.6f,\n" total_carbon;
  print_endline "  \"routes\": [";
  List.iteri
    (fun idx sr ->
       let r = sr.route_data in
       Printf.printf "    {\n";
       Printf.printf "      \"decision\": %s,\n" (json_string sr.decision);
       Printf.printf "      \"score\": %.6f,\n" sr.score;
       Printf.printf "      \"line\": %d,\n" r.source_line;
       Printf.printf "      \"route\": %s,\n" (json_string r.route);
       Printf.printf "      \"provider\": %s,\n" (json_string r.provider);
       Printf.printf "      \"model\": %s,\n" (json_string r.model);
       Printf.printf "      \"region\": %s,\n" (json_string r.region);
       Printf.printf "      \"jurisdiction\": %s,\n" (json_string r.jurisdiction);
       Printf.printf "      \"required_jurisdiction\": %s,\n" (json_string r.required_jurisdiction);
       Printf.printf "      \"data_class\": %s,\n" (json_string r.data_class);
       Printf.printf "      \"monthly_requests\": %.6f,\n" sr.monthly_requests;
       Printf.printf "      \"gross_input_cost_usd\": %.6f,\n" sr.gross_input_cost_usd;
       Printf.printf "      \"gross_output_cost_usd\": %.6f,\n" sr.gross_output_cost_usd;
       Printf.printf "      \"cache_saved_usd\": %.6f,\n" sr.cache_saved_usd;
       Printf.printf "      \"monthly_cost_usd\": %.6f,\n" sr.monthly_cost_usd;
       Printf.printf "      \"monthly_carbon_kg\": %.6f,\n" sr.monthly_carbon_kg;
       Printf.printf "      \"p95_ms\": %.6f,\n" r.p95_ms;
       Printf.printf "      \"p99_ms\": %.6f,\n" r.p99_ms;
       Printf.printf "      \"error_rate\": %.8f,\n" r.error_rate;
       Printf.printf "      \"timeout_rate\": %.8f,\n" r.timeout_rate;
       Printf.printf "      \"cache_hit_rate\": %.8f,\n" r.cache_hit_rate;
       Printf.printf "      \"headroom\": %.8f,\n" sr.headroom;
       Printf.printf "      \"findings\": [";
       List.iteri
         (fun fidx f ->
            if fidx > 0 then print_string ", ";
            Printf.printf "{\"severity\":%s,\"code\":%s,\"message\":%s,\"advice\":%s}"
              (json_string (severity_to_string f.severity))
              (json_string f.code)
              (json_string f.message)
              (json_string f.advice))
         sr.findings;
       Printf.printf "]\n";
       Printf.printf "    }%s\n" (if idx + 1 = List.length scored then "" else ","))
    scored;
  print_endline "  ]";
  print_endline "}"

let emit cfg scored =
  match cfg.output_format with
  | Text -> emit_text scored
  | Csv -> emit_csv scored
  | Json -> emit_json scored

let run () =
  try
    let cfg = parse_args () in
    if cfg.show_example then begin
      print_string example_csv;
      0
    end else begin
      let lines = read_lines cfg.input_path in
      let routes = parse_routes lines in
      let scored = routes |> List.map (score_route cfg) |> sort_scored |> take cfg.top in
      emit cfg scored;
      let worst = max_finding_severity scored in
      if severity_rank worst >= severity_rank cfg.fail_on then 2 else 0
    end
  with
  | Config_error message ->
      prerr_endline ("configuration error: " ^ message);
      prerr_endline "run with --help for usage";
      64
  | Input_error message ->
      prerr_endline ("input error: " ^ message);
      65
  | Sys_error message ->
      prerr_endline ("system error: " ^ message);
      66

let () = exit (run ())

(*
This solves the boring but expensive inference routing problem that starts showing up once a team has several model vendors, prompt caching, private data rules, edge regions, and carbon reporting all fighting each other in the same release gate. Built because by April 2026 a serious AI product cannot just ask which model is cheapest or fastest; the real question is which route is still safe when p99 latency, cache misses, token prices, regional capacity, customer residency, renewable share, and failure rates are measured together. Use it when you export gateway or observability data from LiteLLM, OpenRouter, Vercel AI Gateway, Envoy, Cloudflare Workers AI, Kubernetes inference services, or an internal LLM router and need a deterministic command line check before moving traffic. The trick: it converts every route into the same monthly request, cost, carbon, reliability, cache, and headroom shape, then fails closed on residency and capacity instead of hiding those risks behind a pretty average score. Drop this into a repository as an OCaml inference route budget auditor, AI gateway cost guard, prompt cache efficiency checker, LLM routing SLO validator, carbon aware AI infrastructure report, DevOps release gate, or CI policy file for model routing decisions, and Pavan can read the output without needing a dashboard just to decide whether the next rollout should ship, degrade, or stop.
*)