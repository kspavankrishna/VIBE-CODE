type priority =
  | Bulk
  | Interactive
  | Critical

type data_class =
  | Public
  | Internal
  | Confidential
  | Regulated of string

type routing_mode =
  | RegionalOnly
  | AllowEquivalentJurisdiction
  | AllowAnyRegion

type provider = {
  provider_id : string;
  model : string;
  region : string;
  jurisdiction : string;
  supports_json_schema : bool;
  supports_tool_calls : bool;
  max_context_tokens : int;
  max_output_tokens : int;
  input_microusd_per_mtok : int64;
  output_microusd_per_mtok : int64;
  p50_latency_ms : int;
  p95_latency_ms : int;
  queue_depth : int;
  active_leases : int;
  max_parallel_leases : int;
  error_rate_ppm : int;
  carbon_g_per_kwh : int;
  millijoules_per_token : int;
  cooldown_until_ms : int option;
  accepted_data : data_class list;
  metadata : (string * string) list;
}

type tenant_state = {
  tenant_id : string;
  daily_microusd_remaining : int64;
  monthly_microusd_remaining : int64;
  request_microusd_limit : int64;
  token_budget_remaining : int;
  concurrency_limit : int;
  active_leases : int;
  max_queue_delay_ms : int;
  tier : string;
}

type request = {
  request_id : string;
  tenant_id : string;
  trace_id : string;
  required_model : string;
  data_class : data_class;
  allowed_regions : string list;
  allowed_jurisdictions : string list;
  routing_mode : routing_mode;
  prompt_tokens : int;
  max_completion_tokens : int;
  safety_margin_tokens : int;
  requires_json_schema : bool;
  requires_tool_calls : bool;
  deadline_ms : int;
  now_ms : int;
  max_cost_microusd : int64;
  max_carbon_mg : int64;
  priority : priority;
  idempotency_key : string;
}

type rejection_reason =
  | InvalidRequest of string
  | InvalidProvider of string
  | TenantMismatch
  | TenantBudgetExceeded of string
  | TenantConcurrencyFull
  | ProviderModelMismatch
  | ProviderFeatureMissing of string
  | ProviderContextTooSmall
  | ProviderOutputTooSmall
  | ProviderResidencyBlocked
  | ProviderDataClassBlocked
  | ProviderCoolingDown of int
  | ProviderCapacityFull
  | ProviderHealthTooRisky
  | DeadlineTooTight of int
  | CarbonBudgetExceeded of int64
  | NoViableProvider

type provider_diagnostic = {
  diagnostic_provider_id : string;
  reason : rejection_reason;
  detail : string;
}

type candidate = {
  candidate_provider : provider;
  estimated_cost_microusd : int64;
  estimated_carbon_mg : int64;
  estimated_latency_ms : int;
  score : int64;
  notes : string list;
}

type lease = {
  lease_id : string;
  lease_provider_id : string;
  lease_model : string;
  lease_region : string;
  lease_jurisdiction : string;
  lease_request_id : string;
  lease_tenant_id : string;
  admitted_at_ms : int;
  expires_at_ms : int;
  reserved_tokens : int;
  estimated_cost_microusd : int64;
  estimated_carbon_mg : int64;
  estimated_latency_ms : int;
  score : int64;
  explanation : string;
  log_fields : (string * string) list;
}

type queue_ticket = {
  queue_id : string;
  queue_request_id : string;
  queue_tenant_id : string;
  retry_after_ms : int;
  position_hint : int;
  queue_reason : string;
  diagnostics : provider_diagnostic list;
}

type rejection = {
  rejected_request_id : string;
  rejected_tenant_id : string;
  rejected_reason : rejection_reason;
  diagnostics : provider_diagnostic list;
}

type decision =
  | Admit of lease
  | Queue of queue_ticket
  | Reject of rejection

let trim = String.trim

let non_empty value = trim value <> ""

let max0 value = if value < 0 then 0 else value

let ceil_div_int a b =
  if b <= 0 then invalid_arg "ceil_div_int: denominator must be positive";
  if a <= 0 then 0 else ((a - 1) / b) + 1

let ceil_div_i64 a b =
  if b <= 0L then invalid_arg "ceil_div_i64: denominator must be positive";
  if a <= 0L then 0L else Int64.add (Int64.div (Int64.sub a 1L) b) 1L

let int64_of_nonnegative_int value = Int64.of_int (max0 value)

let priority_to_string = function
  | Bulk -> "bulk"
  | Interactive -> "interactive"
  | Critical -> "critical"

let routing_mode_to_string = function
  | RegionalOnly -> "regional_only"
  | AllowEquivalentJurisdiction -> "allow_equivalent_jurisdiction"
  | AllowAnyRegion -> "allow_any_region"

let data_class_to_string = function
  | Public -> "public"
  | Internal -> "internal"
  | Confidential -> "confidential"
  | Regulated regime -> "regulated:" ^ String.lowercase_ascii (trim regime)

let data_class_rank = function
  | Public -> 0
  | Internal -> 1
  | Confidential -> 2
  | Regulated _ -> 3

let data_class_equal left right =
  match (left, right) with
  | Public, Public -> true
  | Internal, Internal -> true
  | Confidential, Confidential -> true
  | Regulated a, Regulated b -> String.equal (String.lowercase_ascii (trim a)) (String.lowercase_ascii (trim b))
  | _ -> false

let is_regulated = function Regulated _ -> true | _ -> false

let data_class_allowed requested accepted =
  let accepts_by_rank accepted_class =
    match accepted_class with
    | Regulated "*" -> is_regulated requested
    | Regulated _ -> data_class_equal requested accepted_class
    | _ -> (not (is_regulated requested)) && data_class_rank requested <= data_class_rank accepted_class
  in
  List.exists accepts_by_rank accepted

let list_contains needle values = List.exists (String.equal needle) values

let has_residency_hints (req : request) = req.allowed_regions <> [] || req.allowed_jurisdictions <> []

let residency_allowed (req : request) (p : provider) =
  let region_ok = req.allowed_regions = [] || list_contains p.region req.allowed_regions in
  let jurisdiction_ok = req.allowed_jurisdictions = [] || list_contains p.jurisdiction req.allowed_jurisdictions in
  let route_ok =
    match req.routing_mode with
    | AllowAnyRegion -> true
    | RegionalOnly -> region_ok
    | AllowEquivalentJurisdiction ->
        if req.allowed_regions = [] && req.allowed_jurisdictions = [] then true else region_ok || jurisdiction_ok
  in
  route_ok && ((not (is_regulated req.data_class)) || has_residency_hints req)

let total_requested_tokens (req : request) =
  max0 req.prompt_tokens + max0 req.max_completion_tokens + max0 req.safety_margin_tokens

let estimated_cost_microusd (p : provider) (req : request) =
  let input_part = Int64.mul (int64_of_nonnegative_int req.prompt_tokens) p.input_microusd_per_mtok in
  let output_tokens = max0 req.max_completion_tokens + max0 req.safety_margin_tokens in
  let output_part = Int64.mul (int64_of_nonnegative_int output_tokens) p.output_microusd_per_mtok in
  ceil_div_i64 (Int64.add input_part output_part) 1_000_000L

let estimated_carbon_mg (p : provider) (req : request) =
  let token_mj = Int64.mul (int64_of_nonnegative_int (total_requested_tokens req)) (int64_of_nonnegative_int p.millijoules_per_token) in
  let carbon_numerator = Int64.mul token_mj (int64_of_nonnegative_int p.carbon_g_per_kwh) in
  ceil_div_i64 carbon_numerator 3_600_000L

let estimated_latency_ms (p : provider) (req : request) =
  let parallel = max 1 p.max_parallel_leases in
  let open_slots = p.max_parallel_leases - p.active_leases in
  let queue_slots = if open_slots > 0 then 0 else ceil_div_int (p.queue_depth + 1) parallel in
  let queue_penalty = queue_slots * max p.p50_latency_ms (p.p95_latency_ms / 2) in
  let decode_penalty = ceil_div_int (max0 req.max_completion_tokens) 128 * 15 in
  max 0 p.p95_latency_ms + queue_penalty + decode_penalty

let stable_hash value =
  let hash = ref 5381 in
  String.iter
    (fun ch -> hash := (((!hash lsl 5) + !hash) + Char.code ch) land 0x3fffffff)
    value;
  Printf.sprintf "%08x" !hash

let logfmt_escape value =
  let needs_quotes =
    let needs = ref false in
    String.iter
      (fun ch ->
        if ch = ' ' || ch = '\t' || ch = '\n' || ch = '\r' || ch = '"' || ch = '\\' then needs := true)
      value;
    !needs || value = ""
  in
  if not needs_quotes then value
  else
    let buffer = Buffer.create (String.length value + 8) in
    Buffer.add_char buffer '"';
    String.iter
      (function
        | '"' -> Buffer.add_string buffer "\\\""
        | '\\' -> Buffer.add_string buffer "\\\\"
        | '\n' -> Buffer.add_string buffer "\\n"
        | '\r' -> Buffer.add_string buffer "\\r"
        | '\t' -> Buffer.add_string buffer "\\t"
        | ch -> Buffer.add_char buffer ch)
      value;
    Buffer.add_char buffer '"';
    Buffer.contents buffer

let logfmt fields =
  fields
  |> List.map (fun (key, value) -> key ^ "=" ^ logfmt_escape value)
  |> String.concat " "

let reason_to_string = function
  | InvalidRequest _ -> "invalid_request"
  | InvalidProvider _ -> "invalid_provider"
  | TenantMismatch -> "tenant_mismatch"
  | TenantBudgetExceeded _ -> "tenant_budget_exceeded"
  | TenantConcurrencyFull -> "tenant_concurrency_full"
  | ProviderModelMismatch -> "provider_model_mismatch"
  | ProviderFeatureMissing feature -> "provider_feature_missing:" ^ feature
  | ProviderContextTooSmall -> "provider_context_too_small"
  | ProviderOutputTooSmall -> "provider_output_too_small"
  | ProviderResidencyBlocked -> "provider_residency_blocked"
  | ProviderDataClassBlocked -> "provider_data_class_blocked"
  | ProviderCoolingDown _ -> "provider_cooling_down"
  | ProviderCapacityFull -> "provider_capacity_full"
  | ProviderHealthTooRisky -> "provider_health_too_risky"
  | DeadlineTooTight _ -> "deadline_too_tight"
  | CarbonBudgetExceeded _ -> "carbon_budget_exceeded"
  | NoViableProvider -> "no_viable_provider"

let reason_detail = function
  | InvalidRequest message -> message
  | InvalidProvider message -> message
  | TenantBudgetExceeded message -> message
  | ProviderFeatureMissing feature -> feature
  | ProviderCoolingDown until_ms -> "cooldown_until_ms=" ^ string_of_int until_ms
  | DeadlineTooTight latency_ms -> "estimated_latency_ms=" ^ string_of_int latency_ms
  | CarbonBudgetExceeded carbon_mg -> "estimated_carbon_mg=" ^ Int64.to_string carbon_mg
  | reason -> reason_to_string reason

let diagnostic provider reason detail =
  { diagnostic_provider_id = provider; reason; detail }

let validate_request (req : request) =
  let problems = ref [] in
  let add problem = problems := problem :: !problems in
  if not (non_empty req.request_id) then add "request_id is empty";
  if not (non_empty req.tenant_id) then add "tenant_id is empty";
  if not (non_empty req.required_model) then add "required_model is empty";
  if req.prompt_tokens < 0 then add "prompt_tokens is negative";
  if req.max_completion_tokens <= 0 then add "max_completion_tokens must be positive";
  if req.safety_margin_tokens < 0 then add "safety_margin_tokens is negative";
  if req.deadline_ms <= req.now_ms then add "deadline_ms must be after now_ms";
  if Int64.compare req.max_cost_microusd 0L <= 0 then add "max_cost_microusd must be positive";
  if Int64.compare req.max_carbon_mg 0L < 0 then add "max_carbon_mg must not be negative";
  List.rev !problems

let validate_provider (p : provider) =
  let problems = ref [] in
  let add problem = problems := problem :: !problems in
  if not (non_empty p.provider_id) then add "provider_id is empty";
  if not (non_empty p.model) then add "model is empty";
  if not (non_empty p.region) then add "region is empty";
  if not (non_empty p.jurisdiction) then add "jurisdiction is empty";
  if p.max_context_tokens <= 0 then add "max_context_tokens must be positive";
  if p.max_output_tokens <= 0 then add "max_output_tokens must be positive";
  if Int64.compare p.input_microusd_per_mtok 0L < 0 then add "input price is negative";
  if Int64.compare p.output_microusd_per_mtok 0L < 0 then add "output price is negative";
  if p.p50_latency_ms < 0 || p.p95_latency_ms < 0 then add "latency cannot be negative";
  if p.max_parallel_leases <= 0 then add "max_parallel_leases must be positive";
  if p.active_leases < 0 then add "active_leases cannot be negative";
  if p.queue_depth < 0 then add "queue_depth cannot be negative";
  if p.error_rate_ppm < 0 || p.error_rate_ppm > 1_000_000 then add "error_rate_ppm out of range";
  if p.carbon_g_per_kwh < 0 then add "carbon_g_per_kwh cannot be negative";
  if p.millijoules_per_token < 0 then add "millijoules_per_token cannot be negative";
  List.rev !problems

let tenant_fits_cost (tenant : tenant_state) cost =
  Int64.compare cost tenant.daily_microusd_remaining <= 0
  && Int64.compare cost tenant.monthly_microusd_remaining <= 0
  && Int64.compare cost tenant.request_microusd_limit <= 0

let score_candidate (p : provider) (req : request) cost carbon latency =
  let latency_weight, cost_weight, carbon_weight =
    match req.priority with
    | Critical -> (7L, 1L, 1L)
    | Interactive -> (4L, 2L, 2L)
    | Bulk -> (1L, 4L, 5L)
  in
  let load_pct =
    if p.max_parallel_leases <= 0 then 100
    else min 200 ((max0 p.active_leases + max0 p.queue_depth) * 100 / p.max_parallel_leases)
  in
  let latency_score = Int64.mul latency_weight (Int64.of_int latency) in
  let cost_score = Int64.mul cost_weight (Int64.div cost 100L) in
  let carbon_score = Int64.mul carbon_weight (Int64.div carbon 10L) in
  let load_score = Int64.of_int (load_pct * 20) in
  let error_score = Int64.of_int (p.error_rate_ppm / 50) in
  Int64.add latency_score (Int64.add cost_score (Int64.add carbon_score (Int64.add load_score error_score)))

let evaluate_provider (tenant : tenant_state) (req : request) (p : provider) =
  match validate_provider p with
  | problem :: _ -> Error (diagnostic p.provider_id (InvalidProvider problem) problem)
  | [] ->
      if not (String.equal tenant.tenant_id req.tenant_id) then
        Error (diagnostic p.provider_id TenantMismatch "tenant_state.tenant_id does not match request.tenant_id")
      else if not (String.equal p.model req.required_model) then
        Error (diagnostic p.provider_id ProviderModelMismatch ("provider model " ^ p.model))
      else if req.requires_json_schema && not p.supports_json_schema then
        Error (diagnostic p.provider_id (ProviderFeatureMissing "json_schema") "json schema responses required")
      else if req.requires_tool_calls && not p.supports_tool_calls then
        Error (diagnostic p.provider_id (ProviderFeatureMissing "tool_calls") "tool calls required")
      else if total_requested_tokens req > p.max_context_tokens then
        Error (diagnostic p.provider_id ProviderContextTooSmall ("context=" ^ string_of_int p.max_context_tokens))
      else if req.max_completion_tokens > p.max_output_tokens then
        Error (diagnostic p.provider_id ProviderOutputTooSmall ("max_output=" ^ string_of_int p.max_output_tokens))
      else if not (residency_allowed req p) then
        Error (diagnostic p.provider_id ProviderResidencyBlocked ("region=" ^ p.region ^ " jurisdiction=" ^ p.jurisdiction))
      else if not (data_class_allowed req.data_class p.accepted_data) then
        Error (diagnostic p.provider_id ProviderDataClassBlocked (data_class_to_string req.data_class))
      else
        match p.cooldown_until_ms with
        | Some until_ms when until_ms > req.now_ms ->
            Error (diagnostic p.provider_id (ProviderCoolingDown until_ms) ("cooldown_until_ms=" ^ string_of_int until_ms))
        | _ ->
            if p.active_leases >= p.max_parallel_leases then
              Error (diagnostic p.provider_id ProviderCapacityFull "no open provider lease slots")
            else if p.error_rate_ppm >= 250_000 then
              Error (diagnostic p.provider_id ProviderHealthTooRisky ("error_rate_ppm=" ^ string_of_int p.error_rate_ppm))
            else
              let cost = estimated_cost_microusd p req in
              if Int64.compare cost req.max_cost_microusd > 0 then
                Error (diagnostic p.provider_id (TenantBudgetExceeded "request cost cap") (Int64.to_string cost))
              else if not (tenant_fits_cost tenant cost) then
                Error (diagnostic p.provider_id (TenantBudgetExceeded "tenant spend cap") (Int64.to_string cost))
              else if total_requested_tokens req > tenant.token_budget_remaining then
                Error (diagnostic p.provider_id (TenantBudgetExceeded "tenant token cap") (string_of_int (total_requested_tokens req)))
              else
                let carbon = estimated_carbon_mg p req in
                if Int64.compare req.max_carbon_mg 0L > 0 && Int64.compare carbon req.max_carbon_mg > 0 then
                  Error (diagnostic p.provider_id (CarbonBudgetExceeded carbon) (Int64.to_string carbon))
                else
                  let latency = estimated_latency_ms p req in
                  let slack = req.deadline_ms - req.now_ms in
                  if latency > slack then
                    Error (diagnostic p.provider_id (DeadlineTooTight latency) ("slack_ms=" ^ string_of_int slack))
                  else
                    let score = score_candidate p req cost carbon latency in
                    let notes = [
                      "cost_microusd=" ^ Int64.to_string cost;
                      "carbon_mg=" ^ Int64.to_string carbon;
                      "latency_ms=" ^ string_of_int latency;
                      "load=" ^ string_of_int p.active_leases ^ "/" ^ string_of_int p.max_parallel_leases;
                    ] in
                    Ok { candidate_provider = p; estimated_cost_microusd = cost; estimated_carbon_mg = carbon; estimated_latency_ms = latency; score; notes }

let compare_candidate left right =
  let score_cmp = Int64.compare left.score right.score in
  if score_cmp <> 0 then score_cmp
  else
    let latency_cmp = compare left.estimated_latency_ms right.estimated_latency_ms in
    if latency_cmp <> 0 then latency_cmp
    else
      let cost_cmp = Int64.compare left.estimated_cost_microusd right.estimated_cost_microusd in
      if cost_cmp <> 0 then cost_cmp
      else String.compare left.candidate_provider.provider_id right.candidate_provider.provider_id

let choose_candidate candidates =
  match List.sort compare_candidate candidates with
  | best :: _ -> Some best
  | [] -> None

let lease_from_candidate (req : request) (candidate : candidate) =
  let p = candidate.candidate_provider in
  let lease_seed = String.concat ":" [ req.tenant_id; req.request_id; req.idempotency_key; p.provider_id; string_of_int req.now_ms ] in
  let lease_id = "lease_" ^ stable_hash lease_seed in
  let expires_at_ms = req.now_ms + candidate.estimated_latency_ms + 30_000 in
  let reserved_tokens = total_requested_tokens req in
  let explanation =
    String.concat " "
      [ "selected"; p.provider_id; "model=" ^ p.model; "region=" ^ p.region; "because"; String.concat "," candidate.notes ]
  in
  let log_fields = [
    ("event", "inference_lease_admitted");
    ("lease_id", lease_id);
    ("request_id", req.request_id);
    ("tenant_id", req.tenant_id);
    ("trace_id", req.trace_id);
    ("provider", p.provider_id);
    ("model", p.model);
    ("region", p.region);
    ("jurisdiction", p.jurisdiction);
    ("priority", priority_to_string req.priority);
    ("routing", routing_mode_to_string req.routing_mode);
    ("data_class", data_class_to_string req.data_class);
    ("reserved_tokens", string_of_int reserved_tokens);
    ("cost_microusd", Int64.to_string candidate.estimated_cost_microusd);
    ("carbon_mg", Int64.to_string candidate.estimated_carbon_mg);
    ("latency_ms", string_of_int candidate.estimated_latency_ms);
    ("score", Int64.to_string candidate.score);
  ] in
  {
    lease_id;
    lease_provider_id = p.provider_id;
    lease_model = p.model;
    lease_region = p.region;
    lease_jurisdiction = p.jurisdiction;
    lease_request_id = req.request_id;
    lease_tenant_id = req.tenant_id;
    admitted_at_ms = req.now_ms;
    expires_at_ms;
    reserved_tokens;
    estimated_cost_microusd = candidate.estimated_cost_microusd;
    estimated_carbon_mg = candidate.estimated_carbon_mg;
    estimated_latency_ms = candidate.estimated_latency_ms;
    score = candidate.score;
    explanation;
    log_fields;
  }

let queueable_reason = function
  | TenantConcurrencyFull | ProviderCapacityFull | ProviderCoolingDown _ -> true
  | _ -> false

let earliest_retry_ms now diagnostics fallback =
  let from_cooldown =
    diagnostics
    |> List.fold_left
         (fun acc d ->
           match d.reason with
           | ProviderCoolingDown until_ms -> Some (match acc with Some prior -> min prior until_ms | None -> until_ms)
           | _ -> acc)
         None
  in
  match from_cooldown with
  | Some retry -> max now retry
  | None -> now + max 250 fallback

let queue_ticket (tenant : tenant_state) (req : request) diagnostics reason =
  let retry_after_ms = earliest_retry_ms req.now_ms diagnostics (min tenant.max_queue_delay_ms 5_000) in
  let position_hint =
    diagnostics
    |> List.fold_left
         (fun total d -> if queueable_reason d.reason then total + 1 else total)
         1
  in
  {
    queue_id = "queue_" ^ stable_hash (req.tenant_id ^ ":" ^ req.request_id ^ ":" ^ reason);
    queue_request_id = req.request_id;
    queue_tenant_id = req.tenant_id;
    retry_after_ms;
    position_hint;
    queue_reason = reason;
    diagnostics;
  }

let reject req diagnostics reason =
  Reject { rejected_request_id = req.request_id; rejected_tenant_id = req.tenant_id; rejected_reason = reason; diagnostics }

let allocate (providers : provider list) (tenant : tenant_state) (req : request) =
  match validate_request req with
  | problem :: _ -> reject req [] (InvalidRequest problem)
  | [] ->
      if not (String.equal tenant.tenant_id req.tenant_id) then reject req [] TenantMismatch
      else if tenant.active_leases >= tenant.concurrency_limit then
        Queue (queue_ticket tenant req [ diagnostic "tenant" TenantConcurrencyFull "tenant concurrency limit reached" ] "tenant_concurrency_full")
      else
        let candidates, diagnostics =
          List.fold_left
            (fun (candidates, diagnostics) provider ->
              match evaluate_provider tenant req provider with
              | Ok candidate -> (candidate :: candidates, diagnostics)
              | Error diagnostic -> (candidates, diagnostic :: diagnostics))
            ([], [])
            providers
        in
        match choose_candidate candidates with
        | Some candidate -> Admit (lease_from_candidate req candidate)
        | None ->
            let diagnostics = List.rev diagnostics in
            let queueable = List.exists (fun d -> queueable_reason d.reason) diagnostics in
            let hard_budget =
              List.exists
                (fun d ->
                  match d.reason with
                  | TenantBudgetExceeded _ | CarbonBudgetExceeded _ -> true
                  | _ -> false)
                diagnostics
            in
            if queueable && not hard_budget then Queue (queue_ticket tenant req diagnostics "provider_or_tenant_capacity")
            else reject req diagnostics NoViableProvider

let apply_lease (tenant : tenant_state) (providers : provider list) (lease : lease) =
  let tenant' =
    {
      tenant with
      daily_microusd_remaining = Int64.sub tenant.daily_microusd_remaining lease.estimated_cost_microusd;
      monthly_microusd_remaining = Int64.sub tenant.monthly_microusd_remaining lease.estimated_cost_microusd;
      token_budget_remaining = max 0 (tenant.token_budget_remaining - lease.reserved_tokens);
      active_leases = tenant.active_leases + 1;
    }
  in
  let providers' =
    List.map
      (fun p ->
        if String.equal p.provider_id lease.lease_provider_id then { p with active_leases = p.active_leases + 1 }
        else p)
      providers
  in
  (tenant', providers')

let decision_to_logfmt = function
  | Admit lease -> logfmt lease.log_fields
  | Queue ticket ->
      logfmt
        [
          ("event", "inference_lease_queued");
          ("queue_id", ticket.queue_id);
          ("request_id", ticket.queue_request_id);
          ("tenant_id", ticket.queue_tenant_id);
          ("retry_after_ms", string_of_int ticket.retry_after_ms);
          ("position_hint", string_of_int ticket.position_hint);
          ("reason", ticket.queue_reason);
        ]
  | Reject rejection ->
      logfmt
        [
          ("event", "inference_lease_rejected");
          ("request_id", rejection.rejected_request_id);
          ("tenant_id", rejection.rejected_tenant_id);
          ("reason", reason_to_string rejection.rejected_reason);
          ("detail", reason_detail rejection.rejected_reason);
        ]

let diagnostics_to_logfmt diagnostics =
  diagnostics
  |> List.map (fun d ->
         logfmt
           [
             ("provider", d.diagnostic_provider_id);
             ("reason", reason_to_string d.reason);
             ("detail", if d.detail = "" then reason_detail d.reason else d.detail);
           ])

let admitted_provider_id = function
  | Admit lease -> Some lease.lease_provider_id
  | Queue _ | Reject _ -> None

let rejected_reasons = function
  | Reject rejection -> List.map (fun d -> reason_to_string d.reason) rejection.diagnostics
  | Queue ticket -> List.map (fun d -> reason_to_string d.reason) ticket.diagnostics
  | Admit _ -> []

let default_provider ?(provider_id = "edge-a") ?(model = "gpt-4.2-mini") ?(region = "us-east-1") () =
  {
    provider_id;
    model;
    region;
    jurisdiction = "US";
    supports_json_schema = true;
    supports_tool_calls = true;
    max_context_tokens = 128_000;
    max_output_tokens = 16_384;
    input_microusd_per_mtok = 150_000L;
    output_microusd_per_mtok = 600_000L;
    p50_latency_ms = 420;
    p95_latency_ms = 1_200;
    queue_depth = 0;
    active_leases = 0;
    max_parallel_leases = 64;
    error_rate_ppm = 8_000;
    carbon_g_per_kwh = 280;
    millijoules_per_token = 45;
    cooldown_until_ms = None;
    accepted_data = [ Public; Internal; Confidential ];
    metadata = [];
  }

let default_tenant tenant_id =
  {
    tenant_id;
    daily_microusd_remaining = 25_000_000L;
    monthly_microusd_remaining = 500_000_000L;
    request_microusd_limit = 250_000L;
    token_budget_remaining = 2_000_000;
    concurrency_limit = 16;
    active_leases = 0;
    max_queue_delay_ms = 30_000;
    tier = "production";
  }

let default_request ?(request_id = "req_1") ?(tenant_id = "tenant-a") ?(now_ms = 1_000_000) () =
  {
    request_id;
    tenant_id;
    trace_id = "trace_1";
    required_model = "gpt-4.2-mini";
    data_class = Internal;
    allowed_regions = [ "us-east-1"; "us-west-2" ];
    allowed_jurisdictions = [ "US" ];
    routing_mode = AllowEquivalentJurisdiction;
    prompt_tokens = 8_000;
    max_completion_tokens = 1_200;
    safety_margin_tokens = 256;
    requires_json_schema = true;
    requires_tool_calls = true;
    deadline_ms = now_ms + 10_000;
    now_ms;
    max_cost_microusd = 100_000L;
    max_carbon_mg = 1_000L;
    priority = Interactive;
    idempotency_key = "idem_1";
  }

let self_check () =
  let tenant = default_tenant "tenant-a" in
  let req = default_request () in
  let primary = default_provider () in
  let backup = { (default_provider ~provider_id:"edge-b" ~region:"eu-west-1" ()) with jurisdiction = "EU"; p95_latency_ms = 1_800 } in
  begin
    match allocate [ backup; primary ] tenant req with
    | Admit lease ->
        assert (String.equal lease.lease_provider_id "edge-a");
        assert (lease.estimated_cost_microusd > 0L);
        assert (String.length (decision_to_logfmt (Admit lease)) > 32)
    | Queue _ | Reject _ -> assert false
  end;
  let full_tenant = { tenant with active_leases = tenant.concurrency_limit } in
  begin
    match allocate [ primary ] full_tenant req with
    | Queue ticket -> assert (ticket.retry_after_ms > req.now_ms)
    | Admit _ | Reject _ -> assert false
  end;
  let regulated = { req with data_class = Regulated "hipaa"; allowed_regions = []; allowed_jurisdictions = [] } in
  begin
    match allocate [ primary ] tenant regulated with
    | Reject rejection -> assert (String.equal (reason_to_string rejection.rejected_reason) "no_viable_provider")
    | Admit _ | Queue _ -> assert false
  end;
  let too_expensive = { req with max_cost_microusd = 1L } in
  begin
    match allocate [ primary ] tenant too_expensive with
    | Reject rejection -> assert (List.mem "tenant_budget_exceeded" (rejected_reasons (Reject rejection)))
    | Admit _ | Queue _ -> assert false
  end;
  true

let () =
  if Array.length Sys.argv > 1 && String.equal Sys.argv.(1) "--self-check" then
    if self_check () then print_endline "InferenceLeaseAllocator self-check passed"

(*
This solves the April 2026 problem of deciding where an AI inference request should run when teams have real limits: tenant budgets, token quotas, GPU lease slots, model feature gaps, data residency rules, carbon ceilings, provider cooldowns, and user-facing latency deadlines. Built because Pavan would not want another thin router that blindly picks the cheapest endpoint and then fails during a launch, audit, or incident review. Use it when you run an internal AI gateway, MCP proxy, LLM platform, DevOps assistant, research batch runner, edge compute function, or data pipeline that must admit, queue, or reject work before money and context windows get burned. The trick: every provider is scored only after hard checks for model, JSON schema support, tool calls, context size, jurisdiction, data class, spend, health, capacity, deadline, and carbon impact, then the decision is returned with stable lease ids and logfmt fields that are easy to ship into OpenTelemetry, Datadog, ClickHouse, or a plain audit log. Drop this into an OCaml service, a MirageOS edge control plane, a scheduler sidecar, or a build-time policy simulator when you need a production-ready inference lease allocator, AI gateway budget router, carbon-aware LLM scheduler, regional model placement planner, and deterministic DevOps admission controller that is small enough to fork and strict enough for senior engineers to trust.
*)