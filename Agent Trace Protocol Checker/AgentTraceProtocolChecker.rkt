#lang racket/base

;; Agent Trace Protocol Checker
;; Streams a JSONL agent trace and checks it against the tool call protocol.

(require json
         racket/cmdline
         racket/format
         racket/list
         racket/port
         racket/sequence
         racket/string)

(provide (struct-out config)
         (struct-out violation)
         (struct-out result)
         default-config
         check-port
         check-string
         check-file
         result->jsexpr
         format-violation
         exit-code-for
         KNOWN-TYPES
         CODES)

;; ---------------------------------------------------------------------------
;; Configuration

(struct config
  (max-inflight     ; open tool calls allowed per run at once
   max-latency-ms   ; call to result latency above this is a warning
   max-calls        ; tool calls allowed per run
   repeat-limit     ; identical consecutive (tool, args_hash) calls allowed
   skew-ms          ; timestamp regression tolerated before a warning
   allow-truncated  ; a cut off trace (crash, tail -n) is a warning not an error
   max-violations)  ; violations kept in the report, counting never stops
  #:transparent)

(define default-config
  (config 8 120000 500 4 50 #f 1000))

(define KNOWN-TYPES
  '("run_start" "tool_call" "tool_result" "tool_error" "message" "cancel" "run_end"))

;; code -> default severity. Some codes are downgraded at the call site.
(define CODES
  (hash "E_PARSE" 'error
        "E_TRUNCATED" 'warning
        "E_FIELD" 'error
        "E_TYPE" 'error
        "E_START" 'error
        "E_DUP_START" 'error
        "E_AFTER_END" 'error
        "E_SEQ_DUP" 'error
        "E_SEQ_ORDER" 'error
        "E_SEQ_GAP" 'error
        "E_TS_BACKWARDS" 'warning
        "E_CALL_DUP_ID" 'error
        "E_CALL_AFTER_CANCEL" 'error
        "E_ORPHAN_RESULT" 'error
        "E_DOUBLE_RESULT" 'error
        "E_TOOL_MISMATCH" 'error
        "E_LEAK" 'error
        "E_CANCEL_ABANDONED" 'warning
        "E_NO_END" 'error
        "E_CONCURRENCY" 'error
        "E_CALL_BUDGET" 'error
        "E_LATENCY" 'warning
        "E_REPEAT" 'warning))

;; ---------------------------------------------------------------------------
;; Results

(struct violation (severity code file line run seq message) #:transparent)

(struct result
  (file violations counts errors warnings suppressed runs events lines)
  #:transparent)

;; ---------------------------------------------------------------------------
;; Per run state

(struct run-state
  (last-seq last-ts started? ended? cancelled?
   open          ; id -> (vector tool ts seq line)
   seen-ids      ; every id ever used as a tool_call in this run
   calls peak-inflight last-sig repeat-count first-line)
  #:mutable)

(define (new-run-state line)
  (run-state #f #f #f #f #f (make-hash) (make-hash) 0 0 #f 0 line))

;; ---------------------------------------------------------------------------
;; Checker

(struct checker
  (cfg file runs order violations counts errors warnings suppressed events lines kept)
  #:mutable)

(define (new-checker cfg file)
  (checker cfg file (make-hash) '() '() (make-hash) 0 0 0 0 0 0))

(define (add! ck code line run seq msg #:severity [sev #f])
  (define severity (or sev (hash-ref CODES code)))
  (hash-update! (checker-counts ck) code add1 0)
  (if (eq? severity 'error)
      (set-checker-errors! ck (add1 (checker-errors ck)))
      (set-checker-warnings! ck (add1 (checker-warnings ck))))
  (if (< (checker-kept ck) (config-max-violations (checker-cfg ck)))
      (begin
       (set-checker-kept! ck (add1 (checker-kept ck)))
       (set-checker-violations!
       ck (cons (violation severity code (checker-file ck) line run seq msg)
                (checker-violations ck))))
      (set-checker-suppressed! ck (add1 (checker-suppressed ck)))))

(define-syntax-rule (add-and-count! ck code line run seq msg kw ...)
  (add! ck code line run seq msg kw ...))

(define (get-run! ck run line)
  (define runs (checker-runs ck))
  (or (hash-ref runs run #f)
      (let ([st (new-run-state line)])
        (hash-set! runs run st)
        (set-checker-order! ck (cons run (checker-order ck)))
        st)))

(define (whole-number? v)
  (and (real? v) (not (nan? v)) (not (infinite? v)) (integer? v)))

(define (nan? v) (and (flonum? v) (not (= v v))))
(define (infinite? v) (and (flonum? v) (or (= v +inf.0) (= v -inf.0))))

(define (nonempty-string? v) (and (string? v) (positive? (string-length v))))

;; Returns a list of problems with the envelope, empty when it is sound.
(define (envelope-problems ev)
  (append
   (if (nonempty-string? (hash-ref ev 'run #f)) '() '("run must be a non empty string"))
   (let ([s (hash-ref ev 'seq #f)])
     (if (and (whole-number? s) (>= s 0)) '() '("seq must be a non negative integer")))
   (let ([t (hash-ref ev 'ts #f)])
     (if (and (real? t) (not (nan? t)) (not (infinite? t)) (>= t 0))
         '()
         '("ts must be a non negative number of milliseconds")))
   (if (string? (hash-ref ev 'type #f)) '() '("type must be a string"))))

(define (check-event! ck ev line)
  (define cfg (checker-cfg ck))
  (set-checker-events! ck (add1 (checker-events ck)))
  (cond
    [(not (hash? ev))
     (add-and-count! ck "E_FIELD" line #f #f "line is valid JSON but not an object")]
    [else
     (define problems (envelope-problems ev))
     (cond
       [(pair? problems)
        (add-and-count! ck "E_FIELD" line
                        (let ([r (hash-ref ev 'run #f)]) (and (string? r) r))
                        (let ([s (hash-ref ev 'seq #f)]) (and (whole-number? s) (inexact->exact s)))
                        (string-join problems "; "))]
       [else
        (define run (hash-ref ev 'run))
        (define seq (inexact->exact (hash-ref ev 'seq)))
        (define ts (hash-ref ev 'ts))
        (define type (hash-ref ev 'type))
        (define st (get-run! ck run line))
        (cond
          [(run-state-ended? st)
           (add-and-count! ck "E_AFTER_END" line run seq
                           (format "~a event after run_end" type))]
          [else
           (check-sequence! ck st run seq line)
           (check-clock! ck st run seq ts line)
           (unless (member type KNOWN-TYPES)
             (add-and-count! ck "E_TYPE" line run seq (format "unknown event type ~s" type)))
           (check-start! ck st run seq type line)
           (when (member type KNOWN-TYPES)
             (dispatch! ck cfg st ev run seq ts type line))])])]))

(define (check-sequence! ck st run seq line)
  (define last (run-state-last-seq st))
  (cond
    [(not last) (set-run-state-last-seq! st seq)]
    [(= seq last)
     (add-and-count! ck "E_SEQ_DUP" line run seq (format "seq ~a repeated" seq))]
    [(< seq last)
     (add-and-count! ck "E_SEQ_ORDER" line run seq
                     (format "seq ~a arrived after seq ~a" seq last))]
    [else
     (when (> seq (add1 last))
       (add-and-count! ck "E_SEQ_GAP" line run seq
                       (format "seq jumped from ~a to ~a, ~a event(s) missing"
                               last seq (- seq last 1))))
     (set-run-state-last-seq! st seq)]))

(define (check-clock! ck st run seq ts line)
  (define last (run-state-last-ts st))
  (when (and last (< ts (- last (config-skew-ms (checker-cfg ck)))))
    (add-and-count! ck "E_TS_BACKWARDS" line run seq
                    (format "ts ~a is ~a ms behind the previous event"
                            (~r ts #:precision '(= 0)) (~r (- last ts) #:precision '(= 0)))))
  (when (or (not last) (> ts last))
    (set-run-state-last-ts! st ts)))

(define (check-start! ck st run seq type line)
  (cond
    [(equal? type "run_start")
     (if (run-state-started? st)
         (add-and-count! ck "E_DUP_START" line run seq "run_start seen twice")
         (set-run-state-started?! st #t))]
    [(not (run-state-started? st))
     (add-and-count! ck "E_START" line run seq
                     (format "run began with ~a instead of run_start" type))
     (set-run-state-started?! st #t)]))

(define (dispatch! ck cfg st ev run seq ts type line)
  (cond
    [(equal? type "tool_call") (on-call! ck cfg st ev run seq ts line)]
    [(member type '("tool_result" "tool_error")) (on-result! ck cfg st ev run seq ts type line)]
    [(equal? type "cancel") (set-run-state-cancelled?! st #t)]
    [(equal? type "run_end") (on-end! ck st run seq line)]
    [else (void)]))

(define (on-call! ck cfg st ev run seq ts line)
  (define id (hash-ref ev 'id #f))
  (define tool (hash-ref ev 'tool #f))
  (cond
    [(not (and (nonempty-string? id) (nonempty-string? tool)))
     (add-and-count! ck "E_FIELD" line run seq "tool_call needs a non empty id and tool")]
    [else
     (when (run-state-cancelled? st)
       (add-and-count! ck "E_CALL_AFTER_CANCEL" line run seq
                       (format "call ~a to ~a started after cancel" id tool)))
     (cond
       [(hash-ref (run-state-seen-ids st) id #f)
        (add-and-count! ck "E_CALL_DUP_ID" line run seq
                        (format "call id ~a reused, results can no longer be matched" id))]
       [else
        (hash-set! (run-state-seen-ids st) id #t)
        (hash-set! (run-state-open st) id (vector tool ts seq line))
        (set-run-state-calls! st (add1 (run-state-calls st)))
        (when (= (run-state-calls st) (add1 (config-max-calls cfg)))
          (add-and-count! ck "E_CALL_BUDGET" line run seq
                          (format "run passed its budget of ~a tool calls" (config-max-calls cfg))))
        (define open-n (hash-count (run-state-open st)))
        (when (and (> open-n (config-max-inflight cfg))
                   (> open-n (run-state-peak-inflight st)))
          (add-and-count! ck "E_CONCURRENCY" line run seq
                          (format "~a calls in flight, limit is ~a" open-n (config-max-inflight cfg))))
        (set-run-state-peak-inflight! st (max open-n (run-state-peak-inflight st)))
        (track-repeat! ck cfg st ev run seq tool line)])]))

(define (track-repeat! ck cfg st ev run seq tool line)
  (define args-hash (hash-ref ev 'args_hash #f))
  (cond
    [(nonempty-string? args-hash)
     (define sig (cons tool args-hash))
     (if (equal? sig (run-state-last-sig st))
         (set-run-state-repeat-count! st (add1 (run-state-repeat-count st)))
         (begin (set-run-state-last-sig! st sig)
                (set-run-state-repeat-count! st 1)))
     (when (= (run-state-repeat-count st) (add1 (config-repeat-limit cfg)))
       (add-and-count! ck "E_REPEAT" line run seq
                       (format "~a called with identical arguments more than ~a times in a row"
                               tool (config-repeat-limit cfg))))]
    [else
     (set-run-state-last-sig! st #f)
     (set-run-state-repeat-count! st 0)]))

(define (on-result! ck cfg st ev run seq ts type line)
  (define id (hash-ref ev 'id #f))
  (cond
    [(not (nonempty-string? id))
     (add-and-count! ck "E_FIELD" line run seq (format "~a needs a non empty id" type))]
    [(hash-ref (run-state-open st) id #f)
     => (lambda (call)
          (define tool (vector-ref call 0))
          (define claimed (hash-ref ev 'tool #f))
          (define latency (- ts (vector-ref call 1)))
          (hash-remove! (run-state-open st) id)
          (when (and (string? claimed) (not (equal? claimed tool)))
            (add-and-count! ck "E_TOOL_MISMATCH" line run seq
                            (format "result for ~a says tool ~a but the call was to ~a" id claimed tool)))
          (when (> latency (config-max-latency-ms cfg))
            (add-and-count! ck "E_LATENCY" line run seq
                            (format "~a took ~a ms, limit is ~a ms" id
                                    (~r latency #:precision '(= 0)) (config-max-latency-ms cfg)))))]
    [(hash-ref (run-state-seen-ids st) id #f)
     (add-and-count! ck "E_DOUBLE_RESULT" line run seq
                     (format "call ~a already has a result" id))]
    [else
     (add-and-count! ck "E_ORPHAN_RESULT" line run seq
                     (format "result for ~a but no such call was ever made" id))]))

;; Report every call still open, oldest first. `soft` downgrades the
;; severity, used for cancelled runs and for traces that were cut off.
(define (report-open-calls! ck st run line soft-code soft?)
  (define open (sort (hash->list (run-state-open st)) < #:key (lambda (p) (vector-ref (cdr p) 2))))
  (for ([p (in-list open)])
    (define id (car p))
    (define call (cdr p))
    (define msg (format "call ~a to ~a (seq ~a) never received a result"
                        id (vector-ref call 0) (vector-ref call 2)))
    (if soft?
        (add-and-count! ck soft-code (vector-ref call 3) run (vector-ref call 2) msg
                        #:severity 'warning)
        (add-and-count! ck "E_LEAK" (vector-ref call 3) run (vector-ref call 2) msg)))
  (hash-clear! (run-state-open st)))

(define (on-end! ck st run seq line)
  (report-open-calls! ck st run line "E_CANCEL_ABANDONED" (run-state-cancelled? st))
  (set-run-state-ended?! st #t))

(define (finish! ck)
  (define trunc? (config-allow-truncated (checker-cfg ck)))
  (for ([run (in-list (reverse (checker-order ck)))])
    (define st (hash-ref (checker-runs ck) run))
    (unless (run-state-ended? st)
      (add-and-count! ck "E_NO_END" (run-state-first-line st) run #f
                      "trace ended without run_end"
                      #:severity (if trunc? 'warning 'error))
      (report-open-calls! ck st run (run-state-first-line st) "E_LEAK"
                          (or trunc? (run-state-cancelled? st))))))

;; ---------------------------------------------------------------------------
;; Reading

(define (parse-line s)
  ;; Returns (values ok? value-or-message). Rejects trailing garbage after the
  ;; first JSON value, which string->jsexpr on some versions silently accepts.
  (with-handlers ([exn:fail? (lambda (e) (values #f (exn-message e)))])
    (define in (open-input-string s))
    (define v (read-json in))
    (cond
      [(eof-object? v) (values #f "empty JSON document")]
      [(regexp-match? #px"^\\s*$" (port->string in)) (values #t v)]
      [else (values #f "trailing characters after JSON value")])))

(define (check-port in [cfg default-config] #:file [file "<port>"])
  (define ck (new-checker cfg file))
  (let loop ([line-no 1])
    (define raw (read-line in 'any))
    (unless (eof-object? raw)
      (set-checker-lines! ck line-no)
      (define text (string-trim raw))
      (unless (string=? text "")
        (define last? (eof-object? (peek-char in)))
        (define-values (ok? v) (parse-line text))
        (cond
          [ok? (check-event! ck v line-no)]
          [(and last? (config-allow-truncated cfg))
           (add-and-count! ck "E_TRUNCATED" line-no #f #f
                           "final line is cut off, ignoring it")]
          [else
           (add-and-count! ck "E_PARSE" line-no #f #f
                           (format "not valid JSON: ~a"
                                   (let ([m (car (string-split v "\n" #:trim? #f))])
                                     (if (> (string-length m) 120) (substring m 0 120) m))))]))
      (loop (add1 line-no))))
  (finish! ck)
  (result file
          (sort (checker-violations ck) <
                #:key (lambda (v) (or (violation-line v) 0)))
          (checker-counts ck)
          (checker-errors ck)
          (checker-warnings ck)
          (checker-suppressed ck)
          (hash-count (checker-runs ck))
          (checker-events ck)
          (checker-lines ck)))

(define (check-string s [cfg default-config])
  (check-port (open-input-string s) cfg #:file "<string>"))

(define (check-file path [cfg default-config])
  (if (equal? path "-")
      (check-port (current-input-port) cfg #:file "<stdin>")
      (call-with-input-file path
        (lambda (in) (check-port in cfg #:file path)))))

;; ---------------------------------------------------------------------------
;; Output

(define (format-violation v)
  (format "~a:~a: ~a ~a~a~a"
          (violation-file v)
          (or (violation-line v) 0)
          (violation-severity v)
          (violation-code v)
          (let ([r (violation-run v)] [s (violation-seq v)])
            (cond [(and r s) (format " [run=~a seq=~a]" r s)]
                  [r (format " [run=~a]" r)]
                  [else ""]))
          (string-append " " (violation-message v))))

(define (result->jsexpr r)
  (hasheq 'file (result-file r)
          'lines (result-lines r)
          'events (result-events r)
          'runs (result-runs r)
          'errors (result-errors r)
          'warnings (result-warnings r)
          'suppressed (result-suppressed r)
          'byCode (for/hasheq ([(k n) (in-hash (result-counts r))])
                    (values (string->symbol k) n))
          'violations
          (for/list ([v (in-list (result-violations r))])
            (hasheq 'severity (symbol->string (violation-severity v))
                    'code (violation-code v)
                    'line (or (violation-line v) 0)
                    'run (or (violation-run v) 'null)
                    'seq (or (violation-seq v) 'null)
                    'message (violation-message v)))))

(define (exit-code-for results strict?)
  (cond
    [(ormap (lambda (r) (positive? (result-errors r))) results) 1]
    [(and strict? (ormap (lambda (r) (positive? (result-warnings r))) results)) 1]
    [else 0]))

(define (print-text r)
  (for ([v (in-list (result-violations r))])
    (displayln (format-violation v)))
  (when (positive? (result-suppressed r))
    (printf "~a: ~a more violation(s) not shown, raise --max-violations\n"
            (result-file r) (result-suppressed r)))
  (printf "~a: ~a line(s), ~a event(s), ~a run(s), ~a error(s), ~a warning(s)\n"
          (result-file r) (result-lines r) (result-events r)
          (result-runs r) (result-errors r) (result-warnings r)))

;; ---------------------------------------------------------------------------
;; CLI

(module+ main
  (define max-inflight (config-max-inflight default-config))
  (define max-latency (config-max-latency-ms default-config))
  (define max-calls (config-max-calls default-config))
  (define repeat-limit (config-repeat-limit default-config))
  (define skew (config-skew-ms default-config))
  (define max-violations (config-max-violations default-config))
  (define allow-truncated #f)
  (define json? #f)
  (define strict? #f)

  (define (positive-int! flag s)
    (define n (string->number s))
    (unless (and n (exact-nonnegative-integer? n))
      (eprintf "~a needs a non negative integer, got ~a\n" flag s)
      (exit 2))
    n)

  (define files
    (command-line
     #:program "AgentTraceProtocolChecker"
     #:once-each
     [("--max-inflight") n "Concurrent tool calls allowed per run (default 8)"
                         (set! max-inflight (positive-int! "--max-inflight" n))]
     [("--max-latency-ms") n "Call to result latency warning threshold (default 120000)"
                           (set! max-latency (positive-int! "--max-latency-ms" n))]
     [("--max-calls") n "Tool calls allowed per run (default 500)"
                      (set! max-calls (positive-int! "--max-calls" n))]
     [("--repeat-limit") n "Identical consecutive calls allowed (default 4)"
                         (set! repeat-limit (positive-int! "--repeat-limit" n))]
     [("--skew-ms") n "Timestamp regression tolerated in ms (default 50)"
                    (set! skew (positive-int! "--skew-ms" n))]
     [("--max-violations") n "Violations kept in the report (default 1000)"
                           (set! max-violations (positive-int! "--max-violations" n))]
     [("--allow-truncated") "Treat a cut off trace as a warning, not an error"
                            (set! allow-truncated #t)]
     [("--json") "Emit one JSON document instead of text" (set! json? #t)]
     [("--strict") "Exit 1 on warnings as well as errors" (set! strict? #t)]
     #:args files files))

  (when (null? files)
    (eprintf "usage: racket AgentTraceProtocolChecker.rkt [options] <trace.jsonl | ->...\n")
    (exit 2))

  (define cfg (config max-inflight max-latency max-calls repeat-limit skew
                      allow-truncated max-violations))

  (define results
    (for/list ([f (in-list files)])
      (with-handlers ([exn:fail:filesystem?
                       (lambda (e)
                         (eprintf "cannot read ~a: ~a\n" f (exn-message e))
                         (exit 2))])
        (check-file f cfg))))

  (if json?
      (begin (write-json (hasheq 'results (map result->jsexpr results)))
             (newline))
      (for-each print-text results))

  (exit (exit-code-for results strict?)))

;; ---------------------------------------------------------------------------
;; Tests: raco test AgentTraceProtocolChecker.rkt

(module+ test
  (require rackunit)

  (define (ev run seq ts type . kvs)
    (jsexpr->string
     (for/fold ([h (hasheq 'run run 'seq seq 'ts ts 'type (symbol->string type))])
               ([kv (in-slice 2 kvs)])
       (hash-set h (car kv) (cadr kv)))))

  (define (trace . lines) (string-join lines "\n"))

  (define (codes r) (sort (hash-keys (result-counts r)) string<?))

  (define clean
    (trace (ev "a" 0 0 'run_start)
           (ev "a" 1 10 'tool_call 'id "c1" 'tool "search")
           (ev "a" 2 20 'tool_result 'id "c1" 'tool "search")
           (ev "a" 3 30 'run_end)))

  (test-case "clean trace passes"
    (define r (check-string clean))
    (check-equal? (result-errors r) 0)
    (check-equal? (result-warnings r) 0)
    (check-equal? (result-runs r) 1))

  (test-case "leaked call is reported at run_end"
    (define r (check-string
               (trace (ev "a" 0 0 'run_start)
                      (ev "a" 1 1 'tool_call 'id "c1" 'tool "fetch")
                      (ev "a" 2 2 'run_end))))
    (check-equal? (codes r) '("E_LEAK")))

  (test-case "cancel downgrades an abandoned call to a warning"
    (define r (check-string
               (trace (ev "a" 0 0 'run_start)
                      (ev "a" 1 1 'tool_call 'id "c1" 'tool "fetch")
                      (ev "a" 2 2 'cancel)
                      (ev "a" 3 3 'run_end))))
    (check-equal? (codes r) '("E_CANCEL_ABANDONED"))
    (check-equal? (result-errors r) 0))

  (test-case "call after cancel is an error"
    (define r (check-string
               (trace (ev "a" 0 0 'run_start)
                      (ev "a" 1 1 'cancel)
                      (ev "a" 2 2 'tool_call 'id "c1" 'tool "fetch")
                      (ev "a" 3 3 'tool_result 'id "c1")
                      (ev "a" 4 4 'run_end))))
    (check-equal? (codes r) '("E_CALL_AFTER_CANCEL")))

  (test-case "orphan, double result and reused id"
    (define r (check-string
               (trace (ev "a" 0 0 'run_start)
                      (ev "a" 1 1 'tool_result 'id "ghost")
                      (ev "a" 2 2 'tool_call 'id "c1" 'tool "t")
                      (ev "a" 3 3 'tool_result 'id "c1")
                      (ev "a" 4 4 'tool_result 'id "c1")
                      (ev "a" 5 5 'tool_call 'id "c1" 'tool "t")
                      (ev "a" 6 6 'run_end))))
    (check-equal? (codes r) '("E_CALL_DUP_ID" "E_DOUBLE_RESULT" "E_ORPHAN_RESULT")))

  (test-case "tool mismatch"
    (define r (check-string
               (trace (ev "a" 0 0 'run_start)
                      (ev "a" 1 1 'tool_call 'id "c1" 'tool "search")
                      (ev "a" 2 2 'tool_result 'id "c1" 'tool "write")
                      (ev "a" 3 3 'run_end))))
    (check-equal? (codes r) '("E_TOOL_MISMATCH")))

  (test-case "sequence gap, duplicate and reorder"
    (define r (check-string
               (trace (ev "a" 0 0 'run_start)
                      (ev "a" 3 1 'message)
                      (ev "a" 3 2 'message)
                      (ev "a" 2 3 'message)
                      (ev "a" 4 4 'run_end))))
    (check-equal? (codes r) '("E_SEQ_DUP" "E_SEQ_GAP" "E_SEQ_ORDER")))

  (test-case "interleaved runs are tracked independently"
    (define r (check-string
               (trace (ev "a" 0 0 'run_start)
                      (ev "b" 0 0 'run_start)
                      (ev "a" 1 1 'tool_call 'id "c1" 'tool "t")
                      (ev "b" 1 1 'tool_call 'id "c1" 'tool "t")
                      (ev "b" 2 2 'tool_result 'id "c1")
                      (ev "a" 2 2 'tool_result 'id "c1")
                      (ev "a" 3 3 'run_end)
                      (ev "b" 3 3 'run_end))))
    (check-equal? (result-errors r) 0)
    (check-equal? (result-runs r) 2))

  (test-case "concurrency limit reports once per new peak"
    (define cfg (struct-copy config default-config [max-inflight 2]))
    (define r (check-string
               (trace (ev "a" 0 0 'run_start)
                      (ev "a" 1 1 'tool_call 'id "1" 'tool "t")
                      (ev "a" 2 2 'tool_call 'id "2" 'tool "t")
                      (ev "a" 3 3 'tool_call 'id "3" 'tool "t")
                      (ev "a" 4 4 'tool_result 'id "3")
                      (ev "a" 5 5 'tool_call 'id "4" 'tool "t")
                      (ev "a" 6 6 'tool_result 'id "1")
                      (ev "a" 7 7 'tool_result 'id "2")
                      (ev "a" 8 8 'tool_result 'id "4")
                      (ev "a" 9 9 'run_end))
               cfg))
    (check-equal? (hash-ref (result-counts r) "E_CONCURRENCY") 1))

  (test-case "repeat streak warns once"
    (define cfg (struct-copy config default-config [repeat-limit 2]))
    (define lines
      (append (list (ev "a" 0 0 'run_start))
              (for/list ([i (in-range 5)])
                (ev "a" (+ 1 (* 2 i)) (* 2 i) 'tool_call
                    'id (format "c~a" i) 'tool "t" 'args_hash "h"))
              (list (ev "a" 20 20 'run_end))))
    ;; results are missing on purpose, only count the repeat warning
    (define r (check-string (apply trace lines) cfg))
    (check-equal? (hash-ref (result-counts r) "E_REPEAT") 1))

  (test-case "missing run_end and truncated tail"
    (define text
      (string-append (ev "a" 0 0 'run_start) "\n"
                     (ev "a" 1 1 'tool_call 'id "c1" 'tool "t") "\n"
                     "{\"run\":\"a\",\"seq\":2,\"ts\""))
    (define strict (check-string text))
    (check-true (positive? (result-errors strict)))
    (define soft (check-string text (struct-copy config default-config [allow-truncated #t])))
    (check-equal? (result-errors soft) 0)
    (check-equal? (codes soft) '("E_LEAK" "E_NO_END" "E_TRUNCATED")))

  (test-case "bad envelope and bad JSON"
    (define r (check-string (trace "not json"
                                   "[1,2]"
                                   "{\"run\":\"a\",\"seq\":\"x\",\"ts\":1,\"type\":\"message\"}"
                                   "{\"run\":\"a\",\"seq\":0,\"ts\":1,\"type\":\"message\"} junk")))
    (check-equal? (hash-ref (result-counts r) "E_PARSE") 2)
    (check-equal? (hash-ref (result-counts r) "E_FIELD") 2))

  (test-case "events after run_end and missing run_start"
    (define r (check-string
               (trace (ev "a" 0 0 'message)
                      (ev "a" 1 1 'run_end)
                      (ev "a" 2 2 'message))))
    (check-equal? (codes r) '("E_AFTER_END" "E_START")))

  (test-case "timestamp regression respects skew"
    (define r (check-string
               (trace (ev "a" 0 1000 'run_start)
                      (ev "a" 1 980 'message)
                      (ev "a" 2 500 'message)
                      (ev "a" 3 1001 'run_end))))
    (check-equal? (hash-ref (result-counts r) "E_TS_BACKWARDS") 1))

  (test-case "violation cap keeps counting"
    (define cfg (struct-copy config default-config [max-violations 2]))
    (define r (check-string
               (apply trace (for/list ([i (in-range 10)]) (format "junk~a" i)))
               cfg))
    (check-equal? (length (result-violations r)) 2)
    (check-equal? (result-suppressed r) 8)
    (check-equal? (result-errors r) 10))

  (test-case "json report shape"
    (define j (result->jsexpr (check-string clean)))
    (check-equal? (hash-ref j 'errors) 0)
    (check-equal? (hash-ref j 'violations) '())))
