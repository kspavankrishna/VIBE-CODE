#!/usr/bin/env Rscript

options(warn = 1)

EXIT_OK <- 0L
EXIT_FINDINGS <- 2L
EXIT_USAGE <- 64L
EXIT_DATA <- 65L

severity_rank <- c(info = 1L, low = 2L, medium = 3L, high = 4L, critical = 5L)

default_config <- list(
  input = NULL,
  output = NULL,
  format = "text",
  threshold = 0.82,
  ngram = 5L,
  max_pairs = 250000L,
  fail_on = "high",
  example = FALSE,
  show_help = FALSE
)

usage <- function() {
  paste(
    "EvalLeakageSentinel.R - detect AI benchmark leakage and split contamination.",
    "",
    "Usage:",
    "  Rscript EvalLeakageSentinel.R --input eval_items.csv [--format text|csv|json] [--output report.json]",
    "  Rscript EvalLeakageSentinel.R --example --output sample.csv",
    "",
    "Required input columns after normalization:",
    "  item_id, split, prompt",
    "",
    "Useful optional columns:",
    "  response, answer_key, source_url, created_at, owner, model, note",
    "",
    "Options:",
    "  --threshold <0..1>    Jaccard threshold for near-duplicate train/eval text. Default: 0.82",
    "  --ngram <n>           Token shingle size. Default: 5",
    "  --max-pairs <n>       Stop pairwise review after this many candidate pairs. Default: 250000",
    "  --fail-on <level>     info, low, medium, high, critical, or never. Default: high",
    "  --format <fmt>        text, csv, or json. Default: text",
    "  --output <path>       Write report to a file instead of stdout.",
    "  --example             Write an example CSV to --output, or stdout when --output is omitted.",
    "  --help                Show this message.",
    sep = "\n"
  )
}

die <- function(message, status = EXIT_USAGE) {
  writeLines(paste0("error: ", message), con = stderr())
  quit(save = "no", status = status, runLast = FALSE)
}

parse_args <- function(args) {
  cfg <- default_config
  i <- 1L
  while (i <= length(args)) {
    key <- args[[i]]
    needs_value <- function() {
      if (i == length(args)) {
        die(paste("missing value for", key))
      }
      args[[i + 1L]]
    }

    if (key == "--input") {
      cfg$input <- needs_value()
      i <- i + 2L
    } else if (key == "--output") {
      cfg$output <- needs_value()
      i <- i + 2L
    } else if (key == "--format") {
      cfg$format <- tolower(needs_value())
      i <- i + 2L
    } else if (key == "--threshold") {
      cfg$threshold <- suppressWarnings(as.numeric(needs_value()))
      i <- i + 2L
    } else if (key == "--ngram") {
      cfg$ngram <- suppressWarnings(as.integer(needs_value()))
      i <- i + 2L
    } else if (key == "--max-pairs") {
      cfg$max_pairs <- suppressWarnings(as.integer(needs_value()))
      i <- i + 2L
    } else if (key == "--fail-on") {
      cfg$fail_on <- tolower(needs_value())
      i <- i + 2L
    } else if (key == "--example") {
      cfg$example <- TRUE
      i <- i + 1L
    } else if (key %in% c("--help", "-h")) {
      cfg$show_help <- TRUE
      i <- i + 1L
    } else {
      die(paste("unknown argument", key))
    }
  }

  if (!cfg$format %in% c("text", "csv", "json")) {
    die("--format must be text, csv, or json")
  }
  if (is.na(cfg$threshold) || cfg$threshold < 0 || cfg$threshold > 1) {
    die("--threshold must be a number between 0 and 1")
  }
  if (is.na(cfg$ngram) || cfg$ngram < 2L || cfg$ngram > 12L) {
    die("--ngram must be an integer between 2 and 12")
  }
  if (is.na(cfg$max_pairs) || cfg$max_pairs < 1L) {
    die("--max-pairs must be a positive integer")
  }
  if (!cfg$fail_on %in% c(names(severity_rank), "never")) {
    die("--fail-on must be info, low, medium, high, critical, or never")
  }
  cfg
}

write_example_csv <- function(path) {
  example <- data.frame(
    item_id = c("train-001", "train-002", "eval-101", "eval-102", "eval-103"),
    split = c("train", "train", "eval", "holdout", "eval"),
    created_at = c("2026-01-12", "2026-02-03", "2026-03-19", "2025-12-20", "2026-03-20"),
    prompt = c(
      "Summarize latency budgets for an edge model router.",
      "Explain why benchmark answers must stay outside fine-tuning corpora.",
      "Summarize latency budgets for an edge model router with cache fallback.",
      "Explain why benchmark answers must stay outside fine tuning corpora.",
      "Classify a payment retry policy from an incident note."
    ),
    response = c(
      "Route to cached, regional, or deferred inference based on SLO and spend.",
      "A leaked answer key makes the offline score untrustworthy.",
      "Route to cached regional or deferred inference based on SLO and spend.",
      "A leaked answer key makes the offline score untrustworthy.",
      "Use idempotency keys and stop retrying permanent declines."
    ),
    source_url = c(
      "https://example.test/router-a",
      "https://example.test/eval-policy",
      "https://example.test/router-a",
      "https://example.test/eval-policy-copy",
      "https://example.test/retry-policy"
    ),
    owner = c("platform", "research", "evals", "evals", "payments"),
    stringsAsFactors = FALSE
  )

  if (is.null(path)) {
    write.csv(example, stdout(), row.names = FALSE)
  } else {
    write.csv(example, path, row.names = FALSE)
  }
}

normalize_column_names <- function(df) {
  names(df) <- tolower(gsub("_+$", "", gsub("^_+", "", gsub("[^A-Za-z0-9]+", "_", names(df)))))
  df
}

required_column_check <- function(df) {
  required <- c("item_id", "split", "prompt")
  missing <- setdiff(required, names(df))
  if (length(missing) > 0L) {
    die(paste("input is missing required columns:", paste(missing, collapse = ", ")), EXIT_DATA)
  }
}

clean_character <- function(value, default = "") {
  value <- as.character(value)
  value[is.na(value)] <- default
  value
}

coalesce_column <- function(df, column, value = "") {
  if (column %in% names(df)) {
    clean_character(df[[column]], value)
  } else {
    rep(value, nrow(df))
  }
}

read_items <- function(path) {
  if (is.null(path)) {
    die("--input is required unless --example is used")
  }
  if (!file.exists(path)) {
    die(paste("input file does not exist:", path), EXIT_DATA)
  }

  df <- tryCatch(
    read.csv(path, stringsAsFactors = FALSE, check.names = FALSE, na.strings = c("", "NA", "N/A")),
    error = function(e) die(paste("could not read CSV:", conditionMessage(e)), EXIT_DATA)
  )
  if (nrow(df) == 0L) {
    die("input CSV has no rows", EXIT_DATA)
  }
  df <- normalize_column_names(df)
  required_column_check(df)

  df$item_id <- trimws(clean_character(df$item_id))
  df$split <- tolower(trimws(clean_character(df$split)))
  df$prompt <- clean_character(df$prompt)
  df$response <- coalesce_column(df, "response", "")
  df$answer_key <- coalesce_column(df, "answer_key", "")
  df$source_url <- coalesce_column(df, "source_url", "")
  df$created_at <- coalesce_column(df, "created_at", NA_character_)
  df$owner <- coalesce_column(df, "owner", "")
  df$model <- coalesce_column(df, "model", "")
  df$row_number <- seq_len(nrow(df))
  df$text_for_scan <- trimws(paste(df$prompt, df$response, df$answer_key, df$source_url, sep = " "))
  df
}

split_role <- function(split) {
  training <- c("train", "training", "pretrain", "pre_train", "fine_tune", "finetune", "sft", "reference", "context", "source")
  evaluation <- c("eval", "evaluation", "test", "validation", "valid", "holdout", "benchmark", "gold", "canary", "production")
  ifelse(split %in% training, "train", ifelse(split %in% evaluation, "eval", "unknown"))
}

normalize_text <- function(text) {
  text <- clean_character(text)
  text <- tolower(iconv(text, from = "", to = "ASCII//TRANSLIT", sub = " "))
  text[is.na(text)] <- ""
  text <- gsub("https?://[^[:space:]]+", " urltoken ", text, perl = TRUE)
  text <- gsub("[^a-z0-9]+", " ", text, perl = TRUE)
  gsub("\\s+", " ", trimws(text), perl = TRUE)
}

token_shingles <- function(text, n) {
  clean <- normalize_text(text)
  if (!nzchar(clean)) {
    return(character())
  }
  tokens <- unlist(strsplit(clean, " ", fixed = TRUE), use.names = FALSE)
  tokens <- tokens[nzchar(tokens)]
  if (length(tokens) == 0L) {
    return(character())
  }
  if (length(tokens) < n) {
    return(unique(tokens))
  }
  starts <- seq_len(length(tokens) - n + 1L)
  unique(vapply(starts, function(i) paste(tokens[i:(i + n - 1L)], collapse = " "), character(1L)))
}

jaccard <- function(a, b) {
  if (length(a) == 0L || length(b) == 0L) {
    return(0)
  }
  overlap <- length(intersect(a, b))
  if (overlap == 0L) {
    return(0)
  }
  overlap / length(union(a, b))
}

row_signature <- function(row) {
  paste(row$item_id, row$split, row$source_url, sep = " | ")
}

add_finding <- function(findings, severity, code, item_id, other_id, similarity, message, advice) {
  record <- data.frame(
    severity = severity,
    code = code,
    item_id = item_id,
    other_id = other_id,
    similarity = ifelse(is.na(similarity), NA_real_, round(as.numeric(similarity), 4L)),
    message = message,
    advice = advice,
    stringsAsFactors = FALSE
  )
  rbind(findings, record)
}

empty_findings <- function() {
  data.frame(
    severity = character(),
    code = character(),
    item_id = character(),
    other_id = character(),
    similarity = numeric(),
    message = character(),
    advice = character(),
    stringsAsFactors = FALSE
  )
}

candidate_pairs_from_blocks <- function(train_rows, eval_rows, ngram, max_pairs) {
  train_shingles <- lapply(train_rows$text_for_scan, token_shingles, n = ngram)
  eval_shingles <- lapply(eval_rows$text_for_scan, token_shingles, n = ngram)
  train_blocks <- lapply(train_shingles, function(x) head(sort(x), 12L))
  eval_blocks <- lapply(eval_shingles, function(x) head(sort(x), 12L))

  block_index <- new.env(parent = emptyenv(), hash = TRUE)
  for (i in seq_along(train_blocks)) {
    for (block in train_blocks[[i]]) {
      if (!exists(block, envir = block_index, inherits = FALSE)) {
        assign(block, integer(), envir = block_index)
      }
      assign(block, c(get(block, envir = block_index, inherits = FALSE), i), envir = block_index)
    }
  }

  pairs_i <- integer()
  pairs_j <- integer()
  seen <- new.env(parent = emptyenv(), hash = TRUE)
  truncated <- FALSE

  for (j in seq_along(eval_blocks)) {
    candidates <- unique(unlist(lapply(eval_blocks[[j]], function(block) {
      if (exists(block, envir = block_index, inherits = FALSE)) {
        get(block, envir = block_index, inherits = FALSE)
      } else {
        integer()
      }
    }), use.names = FALSE))

    if (length(candidates) == 0L && nrow(train_rows) <= 2000L && nrow(eval_rows) <= 2000L) {
      candidates <- seq_len(nrow(train_rows))
    }

    for (i in candidates) {
      key <- paste(i, j, sep = ":")
      if (exists(key, envir = seen, inherits = FALSE)) {
        next
      }
      assign(key, TRUE, envir = seen)
      pairs_i <- c(pairs_i, i)
      pairs_j <- c(pairs_j, j)
      if (length(pairs_i) >= max_pairs) {
        truncated <- TRUE
        return(list(i = pairs_i, j = pairs_j, train_shingles = train_shingles, eval_shingles = eval_shingles, truncated = truncated))
      }
    }
  }

  list(i = pairs_i, j = pairs_j, train_shingles = train_shingles, eval_shingles = eval_shingles, truncated = truncated)
}

detect_duplicate_ids <- function(items, findings) {
  duplicated_ids <- unique(items$item_id[duplicated(items$item_id) & nzchar(items$item_id)])
  for (id in duplicated_ids) {
    rows <- items[items$item_id == id, , drop = FALSE]
    roles <- paste(unique(rows$role), collapse = ",")
    severity <- if (length(unique(rows$role)) > 1L) "critical" else "high"
    findings <- add_finding(
      findings, severity, "duplicate_item_id", id, "", NA_real_,
      paste("item_id appears", nrow(rows), "times across roles:", roles),
      "Give every benchmark and training row a stable unique item_id before scoring."
    )
  }
  findings
}

detect_source_overlap <- function(train_rows, eval_rows, findings) {
  train_sources <- train_rows[nzchar(train_rows$source_url), c("item_id", "source_url"), drop = FALSE]
  eval_sources <- eval_rows[nzchar(eval_rows$source_url), c("item_id", "source_url"), drop = FALSE]
  if (nrow(train_sources) == 0L || nrow(eval_sources) == 0L) {
    return(findings)
  }
  joined <- merge(eval_sources, train_sources, by = "source_url", suffixes = c("_eval", "_train"))
  if (nrow(joined) == 0L) {
    return(findings)
  }
  for (k in seq_len(nrow(joined))) {
    findings <- add_finding(
      findings, "critical", "source_url_overlap",
      joined$item_id_eval[[k]], joined$item_id_train[[k]], 1,
      paste("eval and training rows share source_url", joined$source_url[[k]]),
      "Remove the shared document from training, rotate the eval item, or label the score as contaminated."
    )
  }
  findings
}

detect_date_boundary <- function(train_rows, eval_rows, findings) {
  train_dates <- suppressWarnings(as.Date(train_rows$created_at))
  eval_dates <- suppressWarnings(as.Date(eval_rows$created_at))
  if (all(is.na(train_dates)) || all(is.na(eval_dates))) {
    return(findings)
  }
  latest_train <- max(train_dates, na.rm = TRUE)
  suspicious <- eval_rows[!is.na(eval_dates) & eval_dates <= latest_train, , drop = FALSE]
  if (nrow(suspicious) == 0L) {
    return(findings)
  }
  for (k in seq_len(nrow(suspicious))) {
    findings <- add_finding(
      findings, "medium", "eval_created_before_training_cutoff",
      suspicious$item_id[[k]], "", NA_real_,
      paste("eval row date", suspicious$created_at[[k]], "is not after latest training date", as.character(latest_train)),
      "Use a clear temporal cutoff and keep post-cutoff eval rows out of pretraining, SFT, RAG fixtures, and synthetic data generation."
    )
  }
  findings
}

detect_empty_or_short_rows <- function(items, findings) {
  empty_prompt <- items[!nzchar(trimws(items$prompt)), , drop = FALSE]
  if (nrow(empty_prompt) > 0L) {
    for (k in seq_len(nrow(empty_prompt))) {
      findings <- add_finding(
        findings, "high", "empty_prompt", empty_prompt$item_id[[k]], "", NA_real_,
        "row has an empty prompt",
        "Drop the row or rebuild it from the source record before it enters eval or training."
      )
    }
  }

  short_eval <- items[items$role == "eval" & nchar(normalize_text(items$prompt)) < 24L, , drop = FALSE]
  if (nrow(short_eval) > 0L) {
    for (k in seq_len(nrow(short_eval))) {
      findings <- add_finding(
        findings, "low", "very_short_eval_prompt", short_eval$item_id[[k]], "", NA_real_,
        "eval prompt is too short to be a reliable leakage fingerprint",
        "Add enough context to support overlap detection, or exclude this item from leakage gating."
      )
    }
  }
  findings
}

detect_answer_in_prompt <- function(items, findings) {
  if (!"answer_key" %in% names(items)) {
    return(findings)
  }
  eval_rows <- items[items$role == "eval" & nzchar(trimws(items$answer_key)), , drop = FALSE]
  for (k in seq_len(nrow(eval_rows))) {
    answer <- normalize_text(eval_rows$answer_key[[k]])
    prompt <- normalize_text(eval_rows$prompt[[k]])
    if (nzchar(answer) && nchar(answer) >= 12L && grepl(answer, prompt, fixed = TRUE)) {
      findings <- add_finding(
        findings, "critical", "answer_key_inside_prompt",
        eval_rows$item_id[[k]], "", 1,
        "answer_key text appears inside the eval prompt",
        "Move answer keys to a private scoring store and regenerate the visible prompt from clean fields."
      )
    }
  }
  findings
}

detect_near_duplicates <- function(train_rows, eval_rows, cfg, findings) {
  if (nrow(train_rows) == 0L || nrow(eval_rows) == 0L) {
    return(list(findings = findings, pair_count = 0L, truncated = FALSE))
  }

  pairs <- candidate_pairs_from_blocks(train_rows, eval_rows, cfg$ngram, cfg$max_pairs)
  pair_count <- length(pairs$i)
  if (pair_count == 0L) {
    return(list(findings = findings, pair_count = 0L, truncated = FALSE))
  }

  for (p in seq_len(pair_count)) {
    train_idx <- pairs$i[[p]]
    eval_idx <- pairs$j[[p]]
    score <- jaccard(pairs$train_shingles[[train_idx]], pairs$eval_shingles[[eval_idx]])
    if (score >= cfg$threshold) {
      severity <- if (score >= 0.94) "critical" else "high"
      findings <- add_finding(
        findings, severity, "near_duplicate_train_eval_text",
        eval_rows$item_id[[eval_idx]], train_rows$item_id[[train_idx]], score,
        paste("eval text is near-duplicate of training text with Jaccard", sprintf("%.3f", score)),
        "Remove the training source, replace the eval item, or quarantine this benchmark before reporting model quality."
      )
    }
  }

  list(findings = findings, pair_count = pair_count, truncated = pairs$truncated)
}

audit_items <- function(items, cfg) {
  items$role <- split_role(items$split)
  findings <- empty_findings()

  unknown <- items[items$role == "unknown", , drop = FALSE]
  if (nrow(unknown) > 0L) {
    for (k in seq_len(nrow(unknown))) {
      findings <- add_finding(
        findings, "medium", "unknown_split_name",
        unknown$item_id[[k]], "", NA_real_,
        paste("split value is not recognized:", unknown$split[[k]]),
        "Normalize split names to train, eval, holdout, test, validation, pretrain, or fine_tune before gating."
      )
    }
  }

  train_rows <- items[items$role == "train", , drop = FALSE]
  eval_rows <- items[items$role == "eval", , drop = FALSE]
  if (nrow(train_rows) == 0L) {
    findings <- add_finding(findings, "high", "missing_training_rows", "", "", NA_real_, "no training/reference rows were found", "Include training, pretrain, fine_tune, or reference rows so leakage can be compared.")
  }
  if (nrow(eval_rows) == 0L) {
    findings <- add_finding(findings, "high", "missing_eval_rows", "", "", NA_real_, "no eval/test/holdout rows were found", "Include eval, test, validation, holdout, benchmark, or gold rows before running the gate.")
  }

  findings <- detect_duplicate_ids(items, findings)
  findings <- detect_empty_or_short_rows(items, findings)
  findings <- detect_answer_in_prompt(items, findings)
  findings <- detect_source_overlap(train_rows, eval_rows, findings)
  findings <- detect_date_boundary(train_rows, eval_rows, findings)
  dupes <- detect_near_duplicates(train_rows, eval_rows, cfg, findings)
  findings <- dupes$findings

  list(
    findings = findings,
    pair_count = dupes$pair_count,
    truncated = dupes$truncated,
    row_count = nrow(items),
    train_count = nrow(train_rows),
    eval_count = nrow(eval_rows),
    unknown_count = nrow(unknown)
  )
}

json_escape <- function(x) {
  x <- ifelse(is.na(x), "", as.character(x))
  x <- gsub("\\\\", "\\\\\\\\", x, perl = TRUE)
  x <- gsub("\"", "\\\\\"", x, perl = TRUE)
  x <- gsub("\n", "\\\\n", x, fixed = TRUE)
  x <- gsub("\r", "\\\\r", x, fixed = TRUE)
  x <- gsub("\t", "\\\\t", x, fixed = TRUE)
  paste0("\"", x, "\"")
}

json_value <- function(x) {
  if (length(x) == 0L || is.na(x)) {
    "null"
  } else if (is.numeric(x) || is.integer(x)) {
    as.character(x)
  } else if (is.logical(x)) {
    tolower(as.character(x))
  } else {
    json_escape(x)
  }
}

findings_to_json <- function(result) {
  f <- result$findings
  rows <- character(nrow(f))
  for (i in seq_len(nrow(f))) {
    fields <- c("severity", "code", "item_id", "other_id", "similarity", "message", "advice")
    values <- vapply(fields, function(field) {
      paste0(json_escape(field), ":", json_value(f[[field]][[i]]))
    }, character(1L))
    rows[[i]] <- paste0("{", paste(values, collapse = ","), "}")
  }
  paste0(
    "{",
    "\"summary\":{",
    "\"row_count\":", result$row_count, ",",
    "\"train_count\":", result$train_count, ",",
    "\"eval_count\":", result$eval_count, ",",
    "\"unknown_count\":", result$unknown_count, ",",
    "\"candidate_pairs_reviewed\":", result$pair_count, ",",
    "\"pair_review_truncated\":", tolower(as.character(result$truncated)), ",",
    "\"finding_count\":", nrow(f),
    "},",
    "\"findings\":[", paste(rows, collapse = ","), "]",
    "}"
  )
}

findings_to_text <- function(result) {
  f <- result$findings
  header <- c(
    "Eval leakage audit",
    paste("Rows:", result$row_count),
    paste("Train rows:", result$train_count),
    paste("Eval rows:", result$eval_count),
    paste("Unknown split rows:", result$unknown_count),
    paste("Candidate pairs reviewed:", result$pair_count),
    paste("Pair review truncated:", result$truncated),
    paste("Findings:", nrow(f)),
    ""
  )

  if (nrow(f) == 0L) {
    return(paste(c(header, "No leakage findings above the configured thresholds."), collapse = "\n"))
  }

  body <- character()
  order_idx <- order(-severity_rank[f$severity], f$code, f$item_id, na.last = TRUE)
  f <- f[order_idx, , drop = FALSE]
  for (i in seq_len(nrow(f))) {
    sim <- ifelse(is.na(f$similarity[[i]]), "n/a", sprintf("%.4f", f$similarity[[i]]))
    body <- c(
      body,
      paste0("[", f$severity[[i]], "] ", f$code[[i]]),
      paste("  item:", ifelse(nzchar(f$item_id[[i]]), f$item_id[[i]], "n/a")),
      paste("  other:", ifelse(nzchar(f$other_id[[i]]), f$other_id[[i]], "n/a")),
      paste("  similarity:", sim),
      paste("  message:", f$message[[i]]),
      paste("  advice:", f$advice[[i]]),
      ""
    )
  }

  paste(c(header, body), collapse = "\n")
}

write_report <- function(result, cfg) {
  text <- switch(
    cfg$format,
    json = findings_to_json(result),
    csv = {
      tmp <- tempfile(fileext = ".csv")
      write.csv(result$findings, tmp, row.names = FALSE, na = "")
      paste(readLines(tmp, warn = FALSE), collapse = "\n")
    },
    text = findings_to_text(result)
  )

  if (is.null(cfg$output)) {
    writeLines(text)
  } else {
    writeLines(text, cfg$output)
  }
}

should_fail <- function(findings, fail_on) {
  if (fail_on == "never" || nrow(findings) == 0L) {
    return(FALSE)
  }
  max(severity_rank[findings$severity], na.rm = TRUE) >= severity_rank[[fail_on]]
}

main <- function() {
  cfg <- parse_args(commandArgs(trailingOnly = TRUE))
  if (cfg$show_help) {
    writeLines(usage())
    quit(save = "no", status = EXIT_OK, runLast = FALSE)
  }
  if (cfg$example) {
    write_example_csv(cfg$output)
    quit(save = "no", status = EXIT_OK, runLast = FALSE)
  }

  items <- read_items(cfg$input)
  result <- audit_items(items, cfg)
  write_report(result, cfg)

  status <- if (should_fail(result$findings, cfg$fail_on)) EXIT_FINDINGS else EXIT_OK
  quit(save = "no", status = status, runLast = FALSE)
}

if (identical(environment(), globalenv())) {
  main()
}

# This solves AI eval leakage detection for teams that keep benchmark rows, RAG fixtures, synthetic data, fine tuning corpora, and model scoring datasets in plain CSV files. Built because April 2026 AI engineering work is full of small research gates where one copied prompt, shared source URL, or old holdout row can make a model look better than it is. Use it when you need a fast Rscript check in CI before publishing eval results, comparing LLM models, shipping an agent benchmark, or accepting a new training data pull request. The trick: it uses plain base R with normalized token shingles, deterministic blocking, source URL checks, duplicate item detection, answer-key scans, and temporal cutoff warnings, so it stays portable without hiding the audit behind a service. Drop this into a repository as an AI benchmark leakage scanner, LLM eval contamination detector, R data quality gate, model evaluation CI check, or research dataset hygiene script, and wire the non-zero exit code to stop contaminated scores before they become a dashboard number.
