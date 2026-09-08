#!/usr/bin/env Rscript

options(warn = 1)

fail <- function(message, status = 64L) {
  cat(sprintf("EvalPowerDrift: %s\n", message), file = stderr())
  quit(save = "no", status = status)
}

usage <- function() {
  cat(paste(
    "Usage:",
    "  Rscript EvalPowerDrift.R --input evals.csv --baseline control --candidate canary [options]",
    "",
    "Required columns:",
    "  variant plus the metric selected with --metric. Optional cost_usd, latency_ms, energy_wh, tokens are analyzed when present.",
    "",
    "Options:",
    "  --variant-col NAME       Variant column, default variant",
    "  --metric NAME            Primary quality metric, default score",
    "  --direction higher|lower Whether larger metric values are better, default higher",
    "  --group-by a,b,c         Grouping columns, default model,route,cohort when present",
    "  --min-n N                Minimum observations per side before PASS, default 30",
    "  --alpha P                Significance threshold, default 0.05",
    "  --mde X                  Minimum detectable effect in metric units",
    "  --fail-underpowered      Exit non-zero when configured MDE power is below 80%",
    "  --json                   Emit machine-readable JSON instead of TSV",
    "", sep = "\n"))
}

read_value <- function(argv, i, flag) {
  if (i >= length(argv)) {
    fail(sprintf("%s requires a value", flag))
  }
  argv[[i + 1L]]
}

parse_list <- function(value) {
  value <- trimws(value)
  if (!nzchar(value) || identical(tolower(value), "none")) {
    return(character())
  }
  parts <- strsplit(value, ",", fixed = TRUE)[[1L]]
  parts <- trimws(parts)
  parts[nzchar(parts)]
}

parse_args <- function(argv) {
  cfg <- list(
    input = "",
    baseline = "control",
    candidate = "candidate",
    variant_col = "variant",
    metric = "score",
    direction = "higher",
    group_by_raw = "model,route,cohort",
    min_n = 30L,
    alpha = 0.05,
    mde = NA_real_,
    json = FALSE,
    fail_underpowered = FALSE
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
    } else if (flag == "--min-n") {
      cfg$min_n <- as.integer(read_value(argv, i, flag)); i <- i + 1L
    } else if (flag == "--alpha") {
      cfg$alpha <- as.numeric(read_value(argv, i, flag)); i <- i + 1L
    } else if (flag == "--mde") {
      cfg$mde <- as.numeric(read_value(argv, i, flag)); i <- i + 1L
    } else if (flag == "--json") {
      cfg$json <- TRUE
    } else if (flag == "--fail-underpowered") {
      cfg$fail_underpowered <- TRUE
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
  if (is.na(cfg$min_n) || cfg$min_n < 2L) {
    fail("--min-n must be at least 2")
  }
  if (is.na(cfg$alpha) || cfg$alpha <= 0 || cfg$alpha >= 1) {
    fail("--alpha must be between 0 and 1")
  }
  cfg$group_by <- parse_list(cfg$group_by_raw)
  cfg
}

as_metric <- function(values, column) {
  numeric_values <- suppressWarnings(as.numeric(values))
  bad <- is.na(numeric_values) & !is.na(values) & nzchar(trimws(as.character(values)))
  if (any(bad)) {
    fail(sprintf("column %s contains non-numeric values", column))
  }
  numeric_values
}

require_columns <- function(data, columns) {
  missing <- setdiff(columns, names(data))
  if (length(missing) > 0L) {
    fail(sprintf("missing required columns: %s", paste(missing, collapse = ", ")))
  }
}

make_key <- function(data, cols) {
  if (length(cols) == 0L) {
    return(rep("all", nrow(data)))
  }
  apply(data[, cols, drop = FALSE], 1L, function(row) paste(row, collapse = "\001"))
}

key_fields <- function(key, cols) {
  if (length(cols) == 0L) {
    return(setNames(list("all"), "scope"))
  }
  values <- strsplit(key, "\001", fixed = TRUE)[[1L]]
  if (length(values) < length(cols)) {
    values <- c(values, rep("", length(cols) - length(values)))
  }
  as.list(setNames(values[seq_along(cols)], cols))
}

welch <- function(base, cand, alpha) {
  base <- base[is.finite(base)]
  cand <- cand[is.finite(cand)]
  n_base <- length(base)
  n_cand <- length(cand)
  mean_base <- if (n_base == 0L) NA_real_ else mean(base)
  mean_cand <- if (n_cand == 0L) NA_real_ else mean(cand)
  var_base <- if (n_base > 1L) var(base) else NA_real_
  var_cand <- if (n_cand > 1L) var(cand) else NA_real_
  delta <- mean_cand - mean_base

  if (n_base < 2L || n_cand < 2L || !is.finite(var_base) || !is.finite(var_cand)) {
    return(list(n_base = n_base, n_cand = n_cand, mean_base = mean_base, mean_cand = mean_cand,
                delta = delta, se = NA_real_, df = NA_real_, p = NA_real_, ci_low = NA_real_,
                ci_high = NA_real_, hedges_g = NA_real_))
  }

  se2 <- var_base / n_base + var_cand / n_cand
  se <- sqrt(se2)
  df_num <- se2 * se2
  df_den <- ((var_base / n_base) ^ 2) / (n_base - 1L) + ((var_cand / n_cand) ^ 2) / (n_cand - 1L)
  df <- df_num / df_den
  t_value <- if (se == 0) 0 else delta / se
  p <- 2 * pt(-abs(t_value), df = df)
  margin <- qt(1 - alpha / 2, df = df) * se

  pooled_den <- n_base + n_cand - 2L
  pooled_sd <- sqrt(((n_base - 1L) * var_base + (n_cand - 1L) * var_cand) / pooled_den)
  correction <- 1 - (3 / (4 * pooled_den - 1))
  hedges_g <- if (pooled_sd == 0) 0 else correction * delta / pooled_sd

  list(n_base = n_base, n_cand = n_cand, mean_base = mean_base, mean_cand = mean_cand,
       delta = delta, se = se, df = df, p = p, ci_low = delta - margin, ci_high = delta + margin,
       hedges_g = hedges_g)
}

power_for_effect <- function(base, cand, effect, alpha) {
  base <- base[is.finite(base)]
  cand <- cand[is.finite(cand)]
  if (length(base) < 2L || length(cand) < 2L || !is.finite(effect)) {
    return(NA_real_)
  }
  se <- sqrt(var(base) / length(base) + var(cand) / length(cand))
  if (!is.finite(se) || se <= 0) {
    return(ifelse(abs(effect) > 0, 1, 0))
  }
  z_alpha <- qnorm(1 - alpha / 2)
  z_effect <- abs(effect) / se
  pnorm(z_effect - z_alpha) + pnorm(-z_effect - z_alpha)
}

classify <- function(primary, optional, cfg, underpowered) {
  enough_n <- primary$n_base >= cfg$min_n && primary$n_cand >= cfg$min_n
  significant <- is.finite(primary$p) && primary$p <= cfg$alpha
  metric_bad <- FALSE
  if (significant && cfg$direction == "higher") {
    metric_bad <- is.finite(primary$delta) && primary$delta < 0
  }
  if (significant && cfg$direction == "lower") {
    metric_bad <- is.finite(primary$delta) && primary$delta > 0
  }
  optional_bad <- any(vapply(optional, function(x) isTRUE(x$bad), logical(1L)))
  if (metric_bad || optional_bad || (cfg$fail_underpowered && underpowered)) {
    return("FAIL")
  }
  if (!enough_n || underpowered) {
    return("WARN")
  }
  "PASS"
}

optional_stats <- function(group, cfg, candidates) {
  out <- list()
  for (name in candidates) {
    if (!name %in% names(group)) next
    group[[name]] <- as_metric(group[[name]], name)
    stat <- welch(group[group[[cfg$variant_col]] == cfg$baseline, name],
                  group[group[[cfg$variant_col]] == cfg$candidate, name], cfg$alpha)
    bad <- is.finite(stat$p) && stat$p <= cfg$alpha && is.finite(stat$delta) && stat$delta > 0
    out[[name]] <- c(stat, list(bad = bad))
  }
  out
}

analyze_group <- function(key, group, cfg) {
  group[[cfg$metric]] <- as_metric(group[[cfg$metric]], cfg$metric)
  primary <- welch(group[group[[cfg$variant_col]] == cfg$baseline, cfg$metric],
                   group[group[[cfg$variant_col]] == cfg$candidate, cfg$metric], cfg$alpha)
  effect_for_power <- if (is.finite(cfg$mde)) cfg$mde else primary$delta
  power <- power_for_effect(group[group[[cfg$variant_col]] == cfg$baseline, cfg$metric],
                            group[group[[cfg$variant_col]] == cfg$candidate, cfg$metric],
                            effect_for_power, cfg$alpha)
  underpowered <- is.finite(power) && power < 0.8
  optional <- optional_stats(group, cfg, c("cost_usd", "latency_ms", "energy_wh", "tokens"))
  fields <- key_fields(key, cfg$group_by)
  status <- classify(primary, optional, cfg, underpowered)

  c(fields, list(
    status = status,
    n_baseline = primary$n_base,
    n_candidate = primary$n_cand,
    metric = cfg$metric,
    baseline_mean = primary$mean_base,
    candidate_mean = primary$mean_cand,
    delta = primary$delta,
    ci_low = primary$ci_low,
    ci_high = primary$ci_high,
    p_value = primary$p,
    hedges_g = primary$hedges_g,
    configured_mde = cfg$mde,
    power = power,
    underpowered = underpowered,
    cost_delta = if (!is.null(optional$cost_usd)) optional$cost_usd$delta else NA_real_,
    latency_delta = if (!is.null(optional$latency_ms)) optional$latency_ms$delta else NA_real_,
    energy_delta = if (!is.null(optional$energy_wh)) optional$energy_wh$delta else NA_real_,
    token_delta = if (!is.null(optional$tokens)) optional$tokens$delta else NA_real_
  ))
}

fmt_num <- function(value) {
  if (length(value) == 0L || is.null(value) || !is.finite(as.numeric(value))) return("NA")
  sprintf("%.6f", as.numeric(value))
}

emit_tsv <- function(rows, cfg) {
  group_cols <- if (length(cfg$group_by) == 0L) "scope" else cfg$group_by
  cols <- c(group_cols, "status", "n_baseline", "n_candidate", "metric", "baseline_mean",
            "candidate_mean", "delta", "ci_low", "ci_high", "p_value", "hedges_g",
            "configured_mde", "power", "underpowered", "cost_delta", "latency_delta",
            "energy_delta", "token_delta")
  cat(paste(cols, collapse = "\t"), "\n", sep = "")
  for (row in rows) {
    values <- vapply(cols, function(col) {
      value <- row[[col]]
      if (is.null(value)) return("")
      if (is.numeric(value)) return(fmt_num(value))
      as.character(value)
    }, character(1L))
    cat(paste(values, collapse = "\t"), "\n", sep = "")
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
  if (is.null(value) || length(value) == 0L) return("null")
  if (is.logical(value)) return(ifelse(isTRUE(value), "true", "false"))
  if (is.numeric(value)) {
    if (!is.finite(value)) return("null")
    return(sprintf("%.10g", value))
  }
  json_escape(value)
}

emit_json <- function(rows, cfg) {
  cat("{\"tool\":\"EvalPowerDrift\",\"metric\":", json_value(cfg$metric),
      ",\"direction\":", json_value(cfg$direction), ",\"groups\":[", sep = "")
  for (i in seq_along(rows)) {
    if (i > 1L) cat(",")
    row <- rows[[i]]
    fields <- names(row)
    cat("{")
    for (j in seq_along(fields)) {
      if (j > 1L) cat(",")
      field <- fields[[j]]
      cat(json_escape(field), ":", json_value(row[[field]]), sep = "")
    }
    cat("}")
  }
  overall <- if (any(vapply(rows, function(row) identical(row$status, "FAIL"), logical(1L)))) "FAIL" else if (any(vapply(rows, function(row) identical(row$status, "WARN"), logical(1L)))) "WARN" else "PASS"
  cat("],\"status\":", json_value(overall), "}\n", sep = "")
}

main <- function() {
  cfg <- parse_args(commandArgs(trailingOnly = TRUE))
  data <- tryCatch(read.csv(cfg$input, stringsAsFactors = FALSE, check.names = FALSE),
                   error = function(e) fail(conditionMessage(e), 66L))
  require_columns(data, c(cfg$variant_col, cfg$metric))
  present_group_by <- cfg$group_by[cfg$group_by %in% names(data)]
  missing_group_by <- setdiff(cfg$group_by, present_group_by)
  if (length(missing_group_by) > 0L) {
    cat(sprintf("EvalPowerDrift: ignoring absent group columns: %s\n", paste(missing_group_by, collapse = ", ")), file = stderr())
  }
  cfg$group_by <- present_group_by

  selected <- data[data[[cfg$variant_col]] %in% c(cfg$baseline, cfg$candidate), , drop = FALSE]
  if (nrow(selected) == 0L) {
    fail("no rows matched the requested baseline and candidate variants")
  }
  keys <- make_key(selected, cfg$group_by)
  groups <- split(selected, keys, drop = TRUE)
  rows <- Map(function(key, group) analyze_group(key, group, cfg), names(groups), groups)

  if (cfg$json) emit_json(rows, cfg) else emit_tsv(rows, cfg)
  statuses <- vapply(rows, function(row) row$status, character(1L))
  quit(save = "no", status = ifelse(any(statuses == "FAIL"), 2L, 0L))
}

main()

# This solves the April 2026 eval launch problem where teams ship model, prompt, RAG, and
# edge inference changes with nice average scores but weak sample sizes, hidden cost drift,
# slower streaming latency, or rising energy use. Built because modern AI developer workflows
# need a small, reviewable, dependency-free R guardrail that can run in CI beside benchmark CSV
# exports from LangSmith, OpenTelemetry, Vercel AI Gateway, batch eval jobs, or internal data
# pipelines. Use it when a canary model or prompt claims to improve quality and you need to ask
# the boring but expensive question: is the lift statistically strong enough, and did it quietly
# increase cost_usd, latency_ms, energy_wh, or token volume by cohort, route, and model? The trick:
# it uses Welch intervals, Hedges effect size, configurable minimum detectable effect power, and
# per-group status decisions without pulling a heavy statistics stack into production. Drop this
# into a repository as EvalPowerDrift.R and it becomes a searchable AI eval power analysis tool,
# LLM benchmark drift detector, model rollout CI gate, prompt regression checker, inference cost
# and carbon-aware DevOps utility, and plain-language audit file that a senior engineer can fork
# quickly when real launch decisions need more than a dashboard screenshot.
