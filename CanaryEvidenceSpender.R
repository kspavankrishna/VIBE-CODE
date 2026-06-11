#!/usr/bin/env Rscript

options(warn = 1)

fail <- function(message, status = 64L) {
  cat(sprintf("CanaryEvidenceSpender: %s\n", message), file = stderr())
  quit(save = "no", status = status)
}

usage <- function() {
  cat(paste(
    "Usage:",
    "  Rscript CanaryEvidenceSpender.R --input evals.csv --baseline control --candidate canary [options]",
    "",
    "Required columns:",
    "  variant column plus the metric selected by --metric. Optional cost_usd, carbon_g, latency_ms,",
    "  and any grouping columns are used when present.",
    "",
    "Core options:",
    "  --variant-col NAME            Variant column, default variant",
    "  --metric NAME                 Primary metric column, default score",
    "  --direction higher|lower      Whether larger metric values are better, default higher",
    "  --group-by a,b,c              Group columns, default task,model,route,cohort when present",
    "  --guardrail-loss X            Allowed candidate loss in oriented metric units, default 0",
    "  --mde X                       Smallest effect worth resolving for more sampling, default auto",
    "  --alpha P                     One-sided decision threshold, default 0.05",
    "  --target-power P              Target planning power, default 0.80",
    "  --min-n N                     Minimum observations per side before final PASS/FAIL, default 30",
    "  --max-n N                     Maximum observations per side per group, default 5000",
    "  --total-budget-usd X          Remaining plus observed total eval budget, default Inf",
    "  --total-carbon-g X            Remaining plus observed carbon budget, default Inf",
    "  --cost-col NAME               Per-row cost column, default cost_usd when present",
    "  --carbon-col NAME             Per-row carbon grams column, default carbon_g when present",
    "  --latency-col NAME            Per-row latency column, default latency_ms when present",
    "  --json                        Emit JSON instead of TSV",
    "  --fail-on-risk                Exit 2 when any group is FAIL or budget_exhausted",
    "", sep = "\n"))
}

read_value <- function(argv, i, flag) {
  if (i >= length(argv)) {
    fail(sprintf("%s requires a value", flag))
  }
  argv[[i + 1L]]
}

parse_number <- function(raw, flag, min_value = -Inf, max_value = Inf) {
  value <- suppressWarnings(as.numeric(raw))
  if (!is.finite(value) || value < min_value || value > max_value) {
    fail(sprintf("%s expects a finite number in [%s, %s], got %s", flag, min_value, max_value, raw))
  }
  value
}

parse_integer <- function(raw, flag, min_value = 1L) {
  value <- suppressWarnings(as.integer(raw))
  if (is.na(value) || value < min_value || as.character(value) != as.character(as.integer(raw))) {
    fail(sprintf("%s expects an integer >= %d, got %s", flag, min_value, raw))
  }
  value
}

parse_list <- function(raw) {
  raw <- trimws(raw)
  if (!nzchar(raw) || identical(tolower(raw), "none")) {
    return(character())
  }
  items <- trimws(strsplit(raw, ",", fixed = TRUE)[[1L]])
  items[nzchar(items)]
}

parse_args <- function(argv) {
  cfg <- list(
    input = "",
    baseline = "control",
    candidate = "canary",
    variant_col = "variant",
    metric = "score",
    direction = "higher",
    group_by_raw = "task,model,route,cohort",
    guardrail_loss = 0.0,
    mde = NA_real_,
    alpha = 0.05,
    target_power = 0.80,
    min_n = 30L,
    max_n = 5000L,
    total_budget_usd = Inf,
    total_carbon_g = Inf,
    cost_col = "",
    carbon_col = "",
    latency_col = "",
    json = FALSE,
    fail_on_risk = FALSE
  )

  i <- 1L
  while (i <= length(argv)) {
    flag <- argv[[i]]
    if (flag %in% c("-h", "--help")) {
      usage()
      quit(save = "no", status = 0L)
    } else if (flag == "--input") {
      cfg$input <- read_value(argv, i, flag); i <- i + 1L
    } else if (flag == "--baseline") {
      cfg$baseline <- read_value(argv, i, flag); i <- i + 1L
    } else if (flag == "--candidate") {
      cfg$candidate <- read_value(argv, i, flag); i <- i + 1L
    } else if (flag == "--variant-col") {
      cfg$variant_col <- read_value(argv, i, flag); i <- i + 1L
    } else if (flag == "--metric") {
      cfg$metric <- read_value(argv, i, flag); i <- i + 1L
    } else if (flag == "--direction") {
      cfg$direction <- tolower(read_value(argv, i, flag)); i <- i + 1L
    } else if (flag == "--group-by") {
      cfg$group_by_raw <- read_value(argv, i, flag); i <- i + 1L
    } else if (flag == "--guardrail-loss") {
      cfg$guardrail_loss <- parse_number(read_value(argv, i, flag), flag, 0); i <- i + 1L
    } else if (flag == "--mde") {
      cfg$mde <- parse_number(read_value(argv, i, flag), flag, 0); i <- i + 1L
    } else if (flag == "--alpha") {
      cfg$alpha <- parse_number(read_value(argv, i, flag), flag, 0.000001, 0.499999); i <- i + 1L
    } else if (flag == "--target-power") {
      cfg$target_power <- parse_number(read_value(argv, i, flag), flag, 0.50, 0.999999); i <- i + 1L
    } else if (flag == "--min-n") {
      cfg$min_n <- parse_integer(read_value(argv, i, flag), flag, 2L); i <- i + 1L
    } else if (flag == "--max-n") {
      cfg$max_n <- parse_integer(read_value(argv, i, flag), flag, 2L); i <- i + 1L
    } else if (flag == "--total-budget-usd") {
      cfg$total_budget_usd <- parse_number(read_value(argv, i, flag), flag, 0); i <- i + 1L
    } else if (flag == "--total-carbon-g") {
      cfg$total_carbon_g <- parse_number(read_value(argv, i, flag), flag, 0); i <- i + 1L
    } else if (flag == "--cost-col") {
      cfg$cost_col <- read_value(argv, i, flag); i <- i + 1L
    } else if (flag == "--carbon-col") {
      cfg$carbon_col <- read_value(argv, i, flag); i <- i + 1L
    } else if (flag == "--latency-col") {
      cfg$latency_col <- read_value(argv, i, flag); i <- i + 1L
    } else if (flag == "--json") {
      cfg$json <- TRUE
    } else if (flag == "--fail-on-risk") {
      cfg$fail_on_risk <- TRUE
    } else {
      fail(sprintf("unknown option %s", flag))
    }
    i <- i + 1L
  }

  if (!nzchar(cfg$input)) {
    usage()
    fail("--input is required")
  }
  if (!file.exists(cfg$input)) {
    fail(sprintf("input file not found: %s", cfg$input), 66L)
  }
  if (!cfg$direction %in% c("higher", "lower")) {
    fail("--direction must be higher or lower")
  }
  if (cfg$max_n < cfg$min_n) {
    fail("--max-n must be greater than or equal to --min-n")
  }
  cfg$group_by_requested <- parse_list(cfg$group_by_raw)
  cfg
}

require_columns <- function(data, columns) {
  missing <- setdiff(columns, names(data))
  if (length(missing) > 0L) {
    fail(sprintf("missing required columns: %s", paste(missing, collapse = ", ")), 65L)
  }
}

numeric_column <- function(data, column, default = NULL) {
  if (!column %in% names(data)) {
    if (is.null(default)) {
      fail(sprintf("missing numeric column %s", column), 65L)
    }
    return(rep(default, nrow(data)))
  }
  raw <- data[[column]]
  out <- suppressWarnings(as.numeric(raw))
  bad <- is.na(out) & !is.na(raw) & nzchar(trimws(as.character(raw)))
  if (any(bad)) {
    fail(sprintf("column %s contains non-numeric values", column), 65L)
  }
  out
}

optional_column <- function(cfg_value, default_name, data) {
  if (nzchar(cfg_value)) {
    return(cfg_value)
  }
  if (default_name %in% names(data)) {
    return(default_name)
  }
  ""
}

orient_metric <- function(values, direction) {
  if (direction == "higher") {
    values
  } else {
    -values
  }
}

make_key <- function(data, cols) {
  if (length(cols) == 0L) {
    return(rep("all", nrow(data)))
  }
  apply(data[, cols, drop = FALSE], 1L, function(row) paste(as.character(row), collapse = "\001"))
}

key_fields <- function(key, cols) {
  if (length(cols) == 0L) {
    return(list(scope = "all"))
  }
  values <- strsplit(key, "\001", fixed = TRUE)[[1L]]
  if (length(values) < length(cols)) {
    values <- c(values, rep("", length(cols) - length(values)))
  }
  as.list(setNames(values[seq_along(cols)], cols))
}

safe_mean <- function(x) {
  x <- x[is.finite(x)]
  if (length(x) == 0L) NA_real_ else mean(x)
}

safe_sum <- function(x) {
  x <- x[is.finite(x)]
  if (length(x) == 0L) 0.0 else sum(x)
}

pooled_sd <- function(a, b) {
  a <- a[is.finite(a)]
  b <- b[is.finite(b)]
  if (length(a) < 2L || length(b) < 2L) {
    return(NA_real_)
  }
  den <- length(a) + length(b) - 2L
  if (den <= 0L) {
    return(NA_real_)
  }
  sqrt(((length(a) - 1L) * var(a) + (length(b) - 1L) * var(b)) / den)
}

welch_stats <- function(base, cand, cfg) {
  base <- base[is.finite(base)]
  cand <- cand[is.finite(cand)]
  n_base <- length(base)
  n_cand <- length(cand)
  mean_base <- if (n_base == 0L) NA_real_ else mean(base)
  mean_cand <- if (n_cand == 0L) NA_real_ else mean(cand)
  effect <- mean_cand - mean_base

  if (n_base < 2L || n_cand < 2L) {
    return(list(
      n_base = n_base, n_cand = n_cand, mean_base = mean_base, mean_cand = mean_cand,
      effect = effect, se = NA_real_, df = NA_real_, ci_low = NA_real_, ci_high = NA_real_,
      p_harm = NA_real_, p_safe = NA_real_, harm_probability = NA_real_, pooled_sd = pooled_sd(base, cand)
    ))
  }

  var_base <- var(base)
  var_cand <- var(cand)
  se2 <- var_base / n_base + var_cand / n_cand
  se <- sqrt(se2)
  df_den <- ((var_base / n_base) ^ 2) / (n_base - 1L) + ((var_cand / n_cand) ^ 2) / (n_cand - 1L)
  df <- if (df_den <= 0 || !is.finite(df_den)) Inf else (se2 * se2) / df_den
  if (!is.finite(se) || se == 0) {
    ci_low <- effect
    ci_high <- effect
    p_harm <- if (effect < -cfg$guardrail_loss) 0 else 1
    p_safe <- if (effect >= -cfg$guardrail_loss) 0 else 1
    harm_probability <- if (effect < -cfg$guardrail_loss) 1 else 0
  } else {
    ci_margin <- qt(1 - cfg$alpha, df = df) * se
    ci_low <- effect - ci_margin
    ci_high <- effect + ci_margin
    t_harm <- (effect + cfg$guardrail_loss) / se
    p_harm <- pt(t_harm, df = df)
    t_safe <- (effect + cfg$guardrail_loss) / se
    p_safe <- 1 - pt(t_safe, df = df)
    harm_probability <- pt((-cfg$guardrail_loss - effect) / se, df = df)
  }

  list(
    n_base = n_base, n_cand = n_cand, mean_base = mean_base, mean_cand = mean_cand,
    effect = effect, se = se, df = df, ci_low = ci_low, ci_high = ci_high,
    p_harm = p_harm, p_safe = p_safe, harm_probability = harm_probability,
    pooled_sd = pooled_sd(base, cand)
  )
}

planned_n_per_arm <- function(stat, cfg) {
  effect_size <- if (is.finite(cfg$mde) && cfg$mde > 0) cfg$mde else max(cfg$guardrail_loss, abs(stat$effect), .Machine$double.eps)
  sd <- stat$pooled_sd
  if (!is.finite(sd) || sd == 0 || !is.finite(effect_size) || effect_size == 0) {
    return(cfg$min_n)
  }
  z_alpha <- qnorm(1 - cfg$alpha)
  z_power <- qnorm(cfg$target_power)
  ceiling(2 * ((z_alpha + z_power) * sd / effect_size) ^ 2)
}

decision_for <- function(stat, cfg) {
  enough <- stat$n_base >= cfg$min_n && stat$n_cand >= cfg$min_n
  if (!enough) {
    return("continue_min_n")
  }
  if (is.finite(stat$p_harm) && stat$p_harm <= cfg$alpha && stat$effect < -cfg$guardrail_loss) {
    return("fail_harm")
  }
  if (is.finite(stat$p_safe) && stat$p_safe <= cfg$alpha && stat$ci_low >= -cfg$guardrail_loss) {
    return("pass_guardrail")
  }
  "continue_uncertain"
}

latency_summary <- function(values) {
  values <- values[is.finite(values)]
  if (length(values) == 0L) {
    return(list(mean = NA_real_, p95 = NA_real_))
  }
  list(mean = mean(values), p95 = unname(quantile(values, 0.95, names = FALSE, type = 7)))
}

analyze_group <- function(key, data, cfg) {
  base_rows <- data[data[[cfg$variant_col]] == cfg$baseline, , drop = FALSE]
  cand_rows <- data[data[[cfg$variant_col]] == cfg$candidate, , drop = FALSE]
  stat <- welch_stats(base_rows$metric_oriented, cand_rows$metric_oriented, cfg)
  target_n <- min(cfg$max_n, max(cfg$min_n, planned_n_per_arm(stat, cfg)))
  current_floor <- min(stat$n_base, stat$n_cand)
  requested_more_per_arm <- max(0L, target_n - current_floor)
  decision <- decision_for(stat, cfg)

  avg_cost <- safe_mean(data$sample_cost_usd)
  avg_carbon <- safe_mean(data$sample_carbon_g)
  if (!is.finite(avg_cost)) avg_cost <- 0.0
  if (!is.finite(avg_carbon)) avg_carbon <- 0.0
  expected_more_usd <- requested_more_per_arm * 2 * avg_cost
  expected_more_carbon_g <- requested_more_per_arm * 2 * avg_carbon
  lat <- latency_summary(data$sample_latency_ms)

  risk_score <- if (is.finite(stat$harm_probability)) stat$harm_probability else 1.0
  if (decision %in% c("pass_guardrail", "fail_harm")) {
    requested_more_per_arm <- 0L
    expected_more_usd <- 0.0
    expected_more_carbon_g <- 0.0
  }

  c(key_fields(key, cfg$group_by), list(
    decision = decision,
    n_baseline = stat$n_base,
    n_candidate = stat$n_cand,
    target_n_per_arm = target_n,
    requested_more_per_arm = requested_more_per_arm,
    allocated_more_per_arm = 0L,
    baseline_mean_oriented = stat$mean_base,
    candidate_mean_oriented = stat$mean_cand,
    effect_oriented = stat$effect,
    guardrail_loss = cfg$guardrail_loss,
    ci_low_one_sided = stat$ci_low,
    ci_high_one_sided = stat$ci_high,
    p_harm = stat$p_harm,
    p_safe = stat$p_safe,
    harm_probability = stat$harm_probability,
    pooled_sd = stat$pooled_sd,
    avg_cost_usd = avg_cost,
    avg_carbon_g = avg_carbon,
    observed_cost_usd = safe_sum(data$sample_cost_usd),
    observed_carbon_g = safe_sum(data$sample_carbon_g),
    expected_more_usd = expected_more_usd,
    expected_more_carbon_g = expected_more_carbon_g,
    latency_mean_ms = lat$mean,
    latency_p95_ms = lat$p95,
    allocation_score = risk_score * sqrt(requested_more_per_arm + 1) / max(avg_cost + avg_carbon / 1000000, 1e-9),
    budget_status = "pending"
  ))
}

allocate_budget <- function(rows, cfg) {
  observed_usd <- sum(vapply(rows, function(row) row$observed_cost_usd, numeric(1L)), na.rm = TRUE)
  observed_carbon <- sum(vapply(rows, function(row) row$observed_carbon_g, numeric(1L)), na.rm = TRUE)
  remaining_usd <- cfg$total_budget_usd - observed_usd
  remaining_carbon <- cfg$total_carbon_g - observed_carbon
  if (!is.finite(remaining_usd)) remaining_usd <- Inf
  if (!is.finite(remaining_carbon)) remaining_carbon <- Inf

  order_idx <- order(vapply(rows, function(row) row$allocation_score, numeric(1L)), decreasing = TRUE, na.last = TRUE)
  for (idx in order_idx) {
    row <- rows[[idx]]
    requested <- as.integer(row$requested_more_per_arm)
    if (requested <= 0L) {
      row$budget_status <- "not_needed"
      rows[[idx]] <- row
      next
    }
    pair_usd <- 2 * row$avg_cost_usd
    pair_carbon <- 2 * row$avg_carbon_g
    max_by_usd <- if (pair_usd <= 0 || is.infinite(remaining_usd)) requested else floor(max(remaining_usd, 0) / pair_usd)
    max_by_carbon <- if (pair_carbon <= 0 || is.infinite(remaining_carbon)) requested else floor(max(remaining_carbon, 0) / pair_carbon)
    grant <- min(requested, max_by_usd, max_by_carbon)
    if (!is.finite(grant)) grant <- requested
    grant <- max(0L, as.integer(grant))
    row$allocated_more_per_arm <- grant
    row$expected_more_usd <- grant * pair_usd
    row$expected_more_carbon_g <- grant * pair_carbon
    row$budget_status <- if (grant >= requested) "funded" else if (grant > 0L) "partial" else "budget_exhausted"
    if (is.finite(remaining_usd)) remaining_usd <- remaining_usd - row$expected_more_usd
    if (is.finite(remaining_carbon)) remaining_carbon <- remaining_carbon - row$expected_more_carbon_g
    rows[[idx]] <- row
  }
  attr(rows, "remaining_usd") <- remaining_usd
  attr(rows, "remaining_carbon_g") <- remaining_carbon
  attr(rows, "observed_usd") <- observed_usd
  attr(rows, "observed_carbon_g") <- observed_carbon
  rows
}

fmt_value <- function(value) {
  if (length(value) == 0L || is.null(value)) return("")
  if (is.logical(value)) return(ifelse(isTRUE(value), "true", "false"))
  if (is.numeric(value)) {
    if (!is.finite(value)) return("NA")
    return(sprintf("%.8g", value))
  }
  as.character(value)
}

emit_tsv <- function(rows, cfg) {
  group_cols <- if (length(cfg$group_by) == 0L) "scope" else cfg$group_by
  cols <- c(
    group_cols, "decision", "budget_status", "n_baseline", "n_candidate",
    "target_n_per_arm", "requested_more_per_arm", "allocated_more_per_arm",
    "effect_oriented", "guardrail_loss", "ci_low_one_sided", "p_harm", "p_safe",
    "harm_probability", "pooled_sd", "avg_cost_usd", "observed_cost_usd",
    "expected_more_usd", "avg_carbon_g", "observed_carbon_g", "expected_more_carbon_g",
    "latency_mean_ms", "latency_p95_ms"
  )
  cat(paste(cols, collapse = "\t"), "\n", sep = "")
  for (row in rows) {
    cat(paste(vapply(cols, function(col) fmt_value(row[[col]]), character(1L)), collapse = "\t"), "\n", sep = "")
  }
}

json_escape <- function(value) {
  value <- as.character(value)
  value <- gsub("\\", "\\\\", value, fixed = TRUE)
  value <- gsub("\"", "\\\"", value, fixed = TRUE)
  value <- gsub("\n", "\\n", value, fixed = TRUE)
  paste0("\"", value, "\"")
}

json_value <- function(value) {
  if (length(value) == 0L || is.null(value)) return("null")
  if (is.logical(value)) return(ifelse(isTRUE(value), "true", "false"))
  if (is.numeric(value)) {
    if (!is.finite(value)) return("null")
    return(sprintf("%.10g", value))
  }
  json_escape(value)
}

emit_json <- function(rows, cfg) {
  cat("{\"tool\":\"CanaryEvidenceSpender\",")
  cat("\"metric\":", json_value(cfg$metric), ",", sep = "")
  cat("\"direction\":", json_value(cfg$direction), ",", sep = "")
  cat("\"observed_usd\":", json_value(attr(rows, "observed_usd")), ",", sep = "")
  cat("\"observed_carbon_g\":", json_value(attr(rows, "observed_carbon_g")), ",", sep = "")
  cat("\"remaining_usd\":", json_value(attr(rows, "remaining_usd")), ",", sep = "")
  cat("\"remaining_carbon_g\":", json_value(attr(rows, "remaining_carbon_g")), ",", sep = "")
  cat("\"groups\":[")
  for (i in seq_along(rows)) {
    if (i > 1L) cat(",")
    row <- rows[[i]]
    names_row <- names(row)
    cat("{")
    for (j in seq_along(names_row)) {
      if (j > 1L) cat(",")
      name <- names_row[[j]]
      cat(json_escape(name), ":", json_value(row[[name]]), sep = "")
    }
    cat("}")
  }
  cat("]}\n")
}

load_data <- function(cfg) {
  data <- tryCatch(
    read.csv(cfg$input, stringsAsFactors = FALSE, check.names = FALSE),
    error = function(e) fail(sprintf("cannot read CSV: %s", conditionMessage(e)), 66L)
  )
  require_columns(data, c(cfg$variant_col, cfg$metric))
  present_variants <- unique(as.character(data[[cfg$variant_col]]))
  if (!cfg$baseline %in% present_variants) {
    fail(sprintf("baseline variant %s not found", cfg$baseline), 65L)
  }
  if (!cfg$candidate %in% present_variants) {
    fail(sprintf("candidate variant %s not found", cfg$candidate), 65L)
  }
  cfg$group_by <- intersect(cfg$group_by_requested, names(data))
  cfg$cost_col <- optional_column(cfg$cost_col, "cost_usd", data)
  cfg$carbon_col <- optional_column(cfg$carbon_col, "carbon_g", data)
  cfg$latency_col <- optional_column(cfg$latency_col, "latency_ms", data)

  metric_values <- numeric_column(data, cfg$metric)
  data$metric_oriented <- orient_metric(metric_values, cfg$direction)
  data$sample_cost_usd <- if (nzchar(cfg$cost_col)) numeric_column(data, cfg$cost_col, 0) else rep(0.0, nrow(data))
  data$sample_carbon_g <- if (nzchar(cfg$carbon_col)) numeric_column(data, cfg$carbon_col, 0) else rep(0.0, nrow(data))
  data$sample_latency_ms <- if (nzchar(cfg$latency_col)) numeric_column(data, cfg$latency_col, NA_real_) else rep(NA_real_, nrow(data))
  data <- data[data[[cfg$variant_col]] %in% c(cfg$baseline, cfg$candidate), , drop = FALSE]
  list(data = data, cfg = cfg)
}

main <- function() {
  cfg <- parse_args(commandArgs(trailingOnly = TRUE))
  loaded <- load_data(cfg)
  data <- loaded$data
  cfg <- loaded$cfg
  keys <- make_key(data, cfg$group_by)
  split_data <- split(data, keys, drop = TRUE)
  rows <- lapply(names(split_data), function(key) analyze_group(key, split_data[[key]], cfg))
  rows <- allocate_budget(rows, cfg)
  if (cfg$json) {
    emit_json(rows, cfg)
  } else {
    emit_tsv(rows, cfg)
  }
  risky <- vapply(rows, function(row) row$decision == "fail_harm" || row$budget_status == "budget_exhausted", logical(1L))
  if (cfg$fail_on_risk && any(risky)) {
    quit(save = "no", status = 2L)
  }
}

tryCatch(
  main(),
  error = function(e) fail(conditionMessage(e), 70L)
)

# This solves the April 2026 AI evaluation problem where teams want to canary a new model,
# retrieval policy, prompt compiler, tool router, or inference gateway but cannot keep spending
# blindly until every cohort has perfect statistical power. Built because Pavan sees eval
# pipelines collect scores, latency, token cost, and carbon estimates, then still make a manual
# gut call about whether to continue, stop, pass, or rollback. Use it when your CSV has baseline
# and candidate rows with a quality metric plus optional cost_usd, carbon_g, latency_ms, model,
# route, task, or cohort columns. The trick: this one R source file orients higher-better and
# lower-better metrics into the same math, applies a one-sided non-inferiority guardrail, estimates
# harm probability, plans the next samples per arm, and greedily allocates scarce eval budget to the
# riskiest unresolved groups instead of averaging away the exact cohort that may break production.
# Drop this into a GitHub Actions eval gate, research notebook, Databricks job, Airflow DAG,
# model gateway canary, LLMOps dashboard, AI agent regression suite, edge inference rollout, or
# DevOps incident workflow when search terms like LLM eval budget allocator, AI canary guardrail,
# model regression evidence planner, carbon aware eval pipeline, sequential A/B testing for prompts,
# and production AI evaluation governance need to point at code that can run with plain Rscript.
