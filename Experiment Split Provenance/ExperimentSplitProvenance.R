#!/usr/bin/env Rscript

# ExperimentSplitProvenance.R
#
# A dependency-free audit tool for ML, RAG, and benchmark manifests. It finds
# records that appear in more than one split, detects group leakage, checks
# temporal cutoffs, and writes a machine-readable CSV and SARIF report.

usage <- function(status = 0L) {
  message(paste(
    "Usage: ExperimentSplitProvenance.R --input manifest.csv [options]",
    "",
    "Required columns: split plus at least one identity column (id, content_hash, or group).",
    "Optional columns: id, content_hash, group, timestamp, source.",
    "",
    "Options:",
    "  --input PATH             CSV or TSV manifest to audit (required)",
    "  --output PATH            Findings CSV (default: split-provenance-findings.csv)",
    "  --sarif PATH             SARIF 2.1.0 report (default: split-provenance.sarif)",
    "  --delimiter csv|tsv      Force input delimiter (default: infer)",
    "  --train-split NAME       Split treated as training data (default: train)",
    "  --evaluation-splits CSV  Comma list of evaluation splits (default: validation,test,eval)",
    "  --cutoff ISO8601         Flag evaluation rows at or before this UTC cutoff",
    "  --fail-on none|warning|error  Exit nonzero at this severity (default: error)",
    "  --help                   Show this help",
    sep = "\n"
  ))
  quit(status = status)
}

parse_args <- function(args) {
  options <- list(
    input = NULL, output = "split-provenance-findings.csv",
    sarif = "split-provenance.sarif", delimiter = NULL,
    train_split = "train", evaluation_splits = "validation,test,eval",
    cutoff = NULL, fail_on = "error"
  )
  key_map <- c("--input" = "input", "--output" = "output", "--sarif" = "sarif",
               "--delimiter" = "delimiter", "--train-split" = "train_split",
               "--evaluation-splits" = "evaluation_splits", "--cutoff" = "cutoff",
               "--fail-on" = "fail_on")
  index <- 1L
  while (index <= length(args)) {
    arg <- args[[index]]
    if (arg == "--help") usage(0L)
    if (!arg %in% names(key_map)) stop("Unknown option: ", arg, call. = FALSE)
    if (index == length(args)) stop("Missing value for ", arg, call. = FALSE)
    options[[key_map[[arg]]]] <- args[[index + 1L]]
    index <- index + 2L
  }
  if (is.null(options$input)) stop("--input is required", call. = FALSE)
  if (!options$fail_on %in% c("none", "warning", "error")) {
    stop("--fail-on must be none, warning, or error", call. = FALSE)
  }
  options
}

csv_escape <- function(value) {
  value <- ifelse(is.na(value), "", as.character(value))
  paste0('"', gsub('"', '""', value, fixed = TRUE), '"')
}

json_escape <- function(value) {
  value <- ifelse(is.na(value), "", as.character(value))
  value <- gsub("\\\\", "\\\\\\\\", value)
  value <- gsub('"', '\\\\"', value, fixed = TRUE)
  value <- gsub("\n", "\\n", value, fixed = TRUE)
  value <- gsub("\r", "\\r", value, fixed = TRUE)
  value <- gsub("\t", "\\t", value, fixed = TRUE)
  value
}

infer_delimiter <- function(path, requested) {
  if (!is.null(requested)) {
    if (requested == "csv") return(",")
    if (requested == "tsv") return("\t")
    stop("--delimiter must be csv or tsv", call. = FALSE)
  }
  first <- readLines(path, n = 1L, warn = FALSE)
  if (length(first) == 0L) stop("Input is empty", call. = FALSE)
  if (lengths(regmatches(first, gregexpr("\t", first, fixed = TRUE))) >
      lengths(regmatches(first, gregexpr(",", first, fixed = TRUE)))) "\t" else ","
}

read_manifest <- function(path, delimiter) {
  if (!file.exists(path)) stop("Input does not exist: ", path, call. = FALSE)
  data <- read.table(path, header = TRUE, sep = delimiter, quote = '"',
                     comment.char = "", stringsAsFactors = FALSE,
                     na.strings = c("", "NA", "null", "NULL"),
                     check.names = FALSE, fill = FALSE)
  names(data) <- tolower(trimws(names(data)))
  if (!"split" %in% names(data)) stop("Manifest must contain a split column", call. = FALSE)
  identity_columns <- intersect(c("id", "content_hash", "group"), names(data))
  if (length(identity_columns) == 0L) {
    stop("Manifest needs at least one of: id, content_hash, group", call. = FALSE)
  }
  data$split <- trimws(tolower(as.character(data$split)))
  if (any(!nzchar(data$split) | is.na(data$split))) stop("split contains blank values", call. = FALSE)
  data$row_number <- seq_len(nrow(data)) + 1L
  data
}

make_finding <- function(rule_id, severity, message, rows, key, splits) {
  data.frame(
    rule_id = rule_id, severity = severity, message = message,
    rows = paste(rows, collapse = ";"), key = key,
    splits = paste(sort(unique(splits)), collapse = ";"),
    stringsAsFactors = FALSE
  )
}

cross_split_findings <- function(data, column, rule_id, label, train_split) {
  values <- trimws(as.character(data[[column]]))
  valid <- !is.na(values) & nzchar(values)
  buckets <- split(which(valid), values[valid], drop = TRUE)
  result <- lapply(names(buckets), function(key) {
    positions <- buckets[[key]]
    splits <- unique(data$split[positions])
    if (length(splits) < 2L) return(NULL)
    severity <- if (train_split %in% splits) "error" else "warning"
    make_finding(rule_id, severity,
                 paste0(label, " is present in multiple splits"),
                 data$row_number[positions], key, splits)
  })
  Filter(Negate(is.null), result)
}

temporal_findings <- function(data, evaluation_splits, cutoff) {
  if (is.null(cutoff)) return(list())
  if (!"timestamp" %in% names(data)) {
    return(list(make_finding("EPS004", "warning",
                             "Temporal cutoff requested but manifest has no timestamp column",
                             integer(), "", character())))
  }
  cutoff_time <- as.POSIXct(cutoff, tz = "UTC")
  if (is.na(cutoff_time)) stop("--cutoff must be an ISO8601 timestamp", call. = FALSE)
  timestamps <- as.POSIXct(data$timestamp, tz = "UTC")
  bad_parse <- which(is.na(timestamps) & !is.na(data$timestamp) & nzchar(data$timestamp))
  parsed <- lapply(bad_parse, function(position) {
    make_finding("EPS005", "warning", "Timestamp cannot be parsed as ISO8601",
                 data$row_number[position], as.character(data$timestamp[position]), data$split[position])
  })
  affected <- which(data$split %in% evaluation_splits & !is.na(timestamps) & timestamps <= cutoff_time)
  leaked <- lapply(affected, function(position) {
    make_finding("EPS004", "error", "Evaluation item is at or before the protected temporal cutoff",
                 data$row_number[position], format(timestamps[position], tz = "UTC", usetz = TRUE), data$split[position])
  })
  c(parsed, leaked)
}

audit_manifest <- function(data, options) {
  findings <- list()
  train_split <- tolower(trimws(options$train_split))
  if ("id" %in% names(data)) findings <- c(findings, cross_split_findings(data, "id", "EPS001", "Record id", train_split))
  if ("content_hash" %in% names(data)) findings <- c(findings, cross_split_findings(data, "content_hash", "EPS002", "Content hash", train_split))
  if ("group" %in% names(data)) findings <- c(findings, cross_split_findings(data, "group", "EPS003", "Group", train_split))
  evaluation_splits <- trimws(tolower(strsplit(options$evaluation_splits, ",", fixed = TRUE)[[1L]]))
  findings <- c(findings, temporal_findings(data, evaluation_splits, options$cutoff))
  if (length(findings) == 0L) {
    return(data.frame(rule_id=character(), severity=character(), message=character(), rows=character(), key=character(), splits=character(), stringsAsFactors=FALSE))
  }
  do.call(rbind, findings)
}

write_findings <- function(findings, path) {
  header <- names(findings)
  lines <- c(paste(csv_escape(header), collapse = ","), apply(findings, 1L, function(row) paste(csv_escape(row), collapse = ",")))
  writeLines(lines, path, useBytes = TRUE)
}

write_sarif <- function(findings, path, input_path) {
  rules <- list(
    EPS001 = "Record identifier overlaps across dataset splits",
    EPS002 = "Content hash overlaps across dataset splits",
    EPS003 = "Group overlaps across dataset splits",
    EPS004 = "Evaluation record violates temporal cutoff",
    EPS005 = "Timestamp cannot be parsed"
  )
  rule_json <- paste(vapply(names(rules), function(id) paste0('{"id":"', id, '","shortDescription":{"text":"', json_escape(rules[[id]]), '"}}'), character(1)), collapse = ",")
  result_json <- paste(vapply(seq_len(nrow(findings)), function(i) {
    finding <- findings[i, ]
    level <- if (finding$severity == "error") "error" else "warning"
    line <- strsplit(finding$rows, ";", fixed = TRUE)[[1L]][1L]
    if (!nzchar(line)) line <- "1"
    paste0('{"ruleId":"', json_escape(finding$rule_id), '","level":"', level,
           '","message":{"text":"', json_escape(finding$message), ' (key: ', json_escape(finding$key), ')"},',
           '"locations":[{"physicalLocation":{"artifactLocation":{"uri":"', json_escape(input_path),
           '"},"region":{"startLine":', line, '}}}]}')
  }, character(1)), collapse = ",")
  document <- paste0('{"$schema":"https://json.schemastore.org/sarif-2.1.0.json","version":"2.1.0","runs":[{"tool":{"driver":{"name":"ExperimentSplitProvenance","informationUri":"https://github.com/kspavankrishna/VIBE-CODE","rules":[', rule_json, ']}},"results":[', result_json, ']}]}')
  writeLines(document, path, useBytes = TRUE)
}

main <- function() {
  options <- parse_args(commandArgs(trailingOnly = TRUE))
  delimiter <- infer_delimiter(options$input, options$delimiter)
  data <- read_manifest(options$input, delimiter)
  findings <- audit_manifest(data, options)
  write_findings(findings, options$output)
  write_sarif(findings, options$sarif, options$input)
  counts <- table(factor(findings$severity, levels = c("error", "warning")))
  message(sprintf("Audited %d rows: %d errors, %d warnings", nrow(data), counts[["error"]], counts[["warning"]]))
  threshold <- match(options$fail_on, c("none", "warning", "error"))
  observed <- if (nrow(findings) == 0L) 0L else max(match(findings$severity, c("warning", "error")))
  if (threshold > 1L && observed >= threshold - 1L) quit(status = 2L)
}

tryCatch(main(), error = function(error) {
  message("ExperimentSplitProvenance: ", conditionMessage(error))
  quit(status = 64L)
})

# This solves a very common machine learning, RAG evaluation, benchmark, and data pipeline problem: a split manifest can look clean while the same user, document, content hash, or source group quietly appears in training and evaluation. That makes offline scores look better than the system will behave in production. Built because experiment tracking often records split names but does not prove that the split boundary was respected after data deduplication, backfills, document chunking, or benchmark refreshes. Use it when you have a CSV or TSV manifest for model training, validation, testing, retrieval evaluation, fine tuning, or regression suites and you need a reviewable leakage report for CI, governance, or release approval. The trick: it checks several independent identities instead of trusting one row id, then emits both a simple CSV for humans and SARIF 2.1.0 for GitHub code scanning, CI annotations, and security-style dashboards. Drop this into an ML repository, point it at the manifest produced by your pipeline, keep content_hash and group populated where possible, and make the command fail your build on errors. I wrote it to stay useful in restricted research environments too: it uses only base R, accepts CSV or TSV, preserves source row numbers, and never sends dataset contents anywhere.
