#lang racket/base

;; Regex Backtrack Prover
;; Finds regexes that blow up on a backtracking engine and proves it with an attack string.

(require json
         racket/cmdline
         racket/list
         racket/port
         racket/string)

(provide (struct-out opts)
         DEFAULT-OPTS
         parse-regex
         run-match
         analyze-pattern
         parse-input-line
         analyze-lines
         render-text
         exit-code-for
         show-string
         FAIL-LEVELS
         main)

;; ---------------------------------------------------------------------------
;; Options

(struct opts
  (max-steps       ; step budget for one reference match
   max-positions   ; Glushkov positions allowed after counted repeats are expanded
   search?         ; #t: engine retries every start offset, like test() or search()
   max-attack      ; longest attack string the prover builds
   ida-budget      ; triple product nodes the polynomial search may expand
   max-witnesses)  ; polynomial witnesses measured before picking the worst
  #:transparent)

(define DEFAULT-OPTS (opts 2000000 300 #t 1024 3000000 8))

(define FAIL-LEVELS '("exponential" "cubic" "quadratic" "suspect" "none"))

(define STEP-TARGET 100000000)

;; ---------------------------------------------------------------------------
;; Code point sets: sorted, merged lists of (lo . hi)

(define MAX-CP #x10FFFF)

(define (norm-set ivs)
  (let loop ([s (sort ivs < #:key car)] [acc '()])
    (cond [(null? s) (reverse acc)]
          [(null? acc) (loop (cdr s) (list (car s)))]
          [else
           (define top (car acc))
           (define x (car s))
           (if (<= (car x) (add1 (cdr top)))
               (loop (cdr s) (cons (cons (car top) (max (cdr top) (cdr x))) (cdr acc)))
               (loop (cdr s) (cons x acc)))])))

(define (set-union a b) (norm-set (append a b)))

(define (set-complement a)
  (let loop ([s a] [lo 0] [acc '()])
    (cond [(null? s)
           (reverse (if (<= lo MAX-CP) (cons (cons lo MAX-CP) acc) acc))]
          [else
           (define x (car s))
           (loop (cdr s)
                 (add1 (cdr x))
                 (if (< lo (car x)) (cons (cons lo (sub1 (car x))) acc) acc))])))

(define (set-intersect a b)
  (let loop ([a a] [b b] [acc '()])
    (cond [(or (null? a) (null? b)) (reverse acc)]
          [else
           (define x (car a))
           (define y (car b))
           (define lo (max (car x) (car y)))
           (define hi (min (cdr x) (cdr y)))
           (define acc* (if (<= lo hi) (cons (cons lo hi) acc) acc))
           (if (< (cdr x) (cdr y))
               (loop (cdr a) b acc*)
               (loop a (cdr b) acc*))])))

(define (set-member? s c)
  (for/or ([iv (in-list s)])
    (and (<= (car iv) c) (<= c (cdr iv)))))

(define (case-fold-set s)
  (define lower (set-intersect s '((97 . 122))))
  (define upper (set-intersect s '((65 . 90))))
  (set-union s
             (set-union (map (lambda (iv) (cons (- (car iv) 32) (- (cdr iv) 32))) lower)
                        (map (lambda (iv) (cons (+ (car iv) 32) (+ (cdr iv) 32))) upper))))

(define DIGIT '((48 . 57)))
(define WORD '((48 . 57) (65 . 90) (95 . 95) (97 . 122)))
(define SPACE '((9 . 13) (32 . 32) (160 . 160) (#x1680 . #x1680) (#x2000 . #x200a)
                (#x2028 . #x2029) (#x202f . #x202f) (#x205f . #x205f) (#x3000 . #x3000)
                (#xfeff . #xfeff)))
(define ALL (list (cons 0 MAX-CP)))
(define DOT-SET (set-complement '((10 . 10) (13 . 13) (#x2028 . #x2029))))

(define (class-escape ch)
  (case ch
    [(#\d) DIGIT]
    [(#\D) (set-complement DIGIT)]
    [(#\w) WORD]
    [(#\W) (set-complement WORD)]
    [(#\s) SPACE]
    [(#\S) (set-complement SPACE)]
    [else #f]))

(define PREFERRED-CHARS
  (string->list "aeiotnsrx0123456789AEIOUbcdfghjklmpqvwyz_-. ,:;@/#!?"))

(define (first-valid-cp set)
  (for/or ([iv (in-list set)])
    (cond [(not (<= #xD800 (car iv) #xDFFF)) (car iv)]
          [(> (cdr iv) #xDFFF) #xE000]
          [else #f])))

(define (pick-char set)
  (or (for/first ([c (in-list PREFERRED-CHARS)]
                  #:when (set-member? set (char->integer c)))
        c)
      (let ([cp (first-valid-cp set)])
        (if cp (integer->char cp) #\?))))

;; ---------------------------------------------------------------------------
;; AST

(struct Chars (set span))
(struct Cat (items))
(struct Alt (items))
(struct Rep (node min max greedy))
(struct Grp (node idx))
(struct Atomic (node))
(struct Look (node neg? behind?))
(struct Backref (n))
(struct Bol ())
(struct Eol ())
(struct WordB (neg?))
(struct Empty ())

(struct exn:regex exn:fail ())

(define (rx-error fmt . args)
  (raise (exn:regex (apply format fmt args) (current-continuation-marks))))

;; ---------------------------------------------------------------------------
;; Parser: JavaScript flavoured syntax plus the PCRE extras people paste in
;; (atomic groups, possessive quantifiers, named groups with ?P<).

(define (parse-regex src #:icase? [icase? #f] #:dotall? [dotall? #f])
  (define n (string-length src))
  (define pos 0)
  (define ngroups 0)
  (define max-backref 0)
  (define names (make-hash))
  (define notes '())

  (define (note! s) (unless (member s notes) (set! notes (cons s notes))))
  (define (peek) (and (< pos n) (string-ref src pos)))
  (define (peek-at k) (and (< (+ pos k) n) (string-ref src (+ pos k))))
  (define (advance!) (begin0 (string-ref src pos) (set! pos (add1 pos))))
  (define (eat! c) (and (eqv? (peek) c) (begin (set! pos (add1 pos)) #t)))
  (define (eat-str! s)
    (define e (+ pos (string-length s)))
    (and (<= e n) (string=? (substring src pos e) s) (begin (set! pos e) #t)))
  (define (leaf set start)
    (Chars (if icase? (case-fold-set set) set) (cons start pos)))
  (define (single cp) (list (cons cp cp)))

  (define (read-hex digits)
    (define e (+ pos digits))
    (unless (and (<= e n) (regexp-match? #px"^[0-9a-fA-F]+$" (substring src pos e)))
      (rx-error "bad hex escape at offset ~a" pos))
    (begin0 (string->number (substring src pos e) 16) (set! pos e)))

  ;; after the backslash, returns a code point
  (define (read-escape-char)
    (define c (advance!))
    (case c
      [(#\n) 10] [(#\r) 13] [(#\t) 9] [(#\f) 12] [(#\v) 11]
      [(#\0) 0]
      [(#\x) (read-hex 2)]
      [(#\u)
       (cond [(eat! #\{)
              (define m (regexp-match #px"^([0-9a-fA-F]+)\\}" src pos))
              (unless m (rx-error "bad unicode escape at offset ~a" pos))
              (set! pos (+ pos (string-length (car m))))
              (define cp (string->number (cadr m) 16))
              (when (> cp MAX-CP) (rx-error "code point out of range at offset ~a" pos))
              cp]
             [else (read-hex 4)])]
      [(#\c)
       (define l (peek))
       (cond [(and l (char-alphabetic? l)) (advance!) (modulo (char->integer l) 32)]
             [else (char->integer #\c)])]
      [else (char->integer c)]))

  (define (skip-property!)
    (advance!)
    (cond [(eat! #\{)
           (unless (regexp-match? #px"^[^}]*\\}" src pos) (rx-error "bad property escape at offset ~a" pos))
           (let loop () (unless (eqv? (advance!) #\}) (loop)))]
          [else (void)])
    (note! "property escapes are approximated as any character"))

  (define (read-name!)
    (define m (regexp-match #px"^([A-Za-z_][A-Za-z0-9_]*)>" src pos))
    (unless m (rx-error "bad group name at offset ~a" pos))
    (set! pos (+ pos (string-length (car m))))
    (cadr m))

  (define (parse-alt)
    (let loop ([branches (list (parse-cat))])
      (if (eat! #\|)
          (loop (cons (parse-cat) branches))
          (if (null? (cdr branches)) (car branches) (Alt (reverse branches))))))

  (define (parse-cat)
    (let loop ([items '()])
      (define c (peek))
      (if (or (not c) (eqv? c #\|) (eqv? c #\)))
          (cond [(null? items) (Empty)]
                [(null? (cdr items)) (car items)]
                [else (Cat (reverse items))])
          (loop (cons (parse-quantified) items)))))

  (define (parse-quantified)
    (define start pos)
    (define atom (parse-atom))
    (define q (parse-quantifier))
    (cond
      [(not q) atom]
      [(or (Bol? atom) (Eol? atom)) (rx-error "nothing to repeat at offset ~a" start)]
      [else
       (define r (Rep atom (first q) (second q) (third q)))
       (cond [(fourth q) (note! "possessive quantifier") (Atomic r)]
             [else r])]))

  (define (parse-quantifier)
    (define c (peek))
    (define-values (mn mx)
      (cond
        [(eqv? c #\*) (advance!) (values 0 #f)]
        [(eqv? c #\+) (advance!) (values 1 #f)]
        [(eqv? c #\?) (advance!) (values 0 1)]
        [(eqv? c #\{)
         (define m (regexp-match #px"^\\{([0-9]+)(?:(,)([0-9]*))?\\}" src pos))
         (cond
           [m
            (set! pos (+ pos (string-length (car m))))
            (define lo (string->number (cadr m)))
            (define hi (cond [(not (caddr m)) lo]
                             [(string=? (cadddr m) "") #f]
                             [else (string->number (cadddr m))]))
            (when (or (> lo 100000) (and hi (> hi 100000)))
              (rx-error "quantifier bound too large"))
            (when (and hi (< hi lo)) (rx-error "quantifier range out of order"))
            (values lo hi)]
           [else (values #f #f)])]
        [else (values #f #f)]))
    (and mn
         (let* ([lazy? (eat! #\?)]
                [poss? (and (not lazy?) (eat! #\+))])
           (list mn mx (not lazy?) poss?))))

  (define (parse-atom)
    (define start pos)
    (define c (advance!))
    (case c
      [(#\() (parse-group start)]
      [(#\[) (parse-class start)]
      [(#\.) (leaf (if dotall? ALL DOT-SET) start)]
      [(#\^) (Bol)]
      [(#\$) (Eol)]
      [(#\\) (parse-escape start)]
      [(#\* #\+ #\?) (rx-error "nothing to repeat at offset ~a" start)]
      [(#\{)
       (when (regexp-match? #px"^[0-9]+(?:,[0-9]*)?\\}" src pos)
         (rx-error "nothing to repeat at offset ~a" start))
       (leaf (single (char->integer c)) start)]
      [else (leaf (single (char->integer c)) start)]))

  (define (parse-group start)
    (define (finish make)
      (define inner (parse-alt))
      (unless (eat! #\)) (rx-error "missing ) for group opened at offset ~a" start))
      (make inner))
    (define (capture! name)
      (set! ngroups (add1 ngroups))
      (when name (hash-set! names name ngroups))
      ngroups)
    (cond
      [(eat! #\?)
       (cond
         [(eat! #\:) (finish values)]
         [(eat! #\=) (note! "lookahead is not modelled by the static analysis")
                     (finish (lambda (x) (Look x #f #f)))]
         [(eat! #\!) (note! "lookahead is not modelled by the static analysis")
                     (finish (lambda (x) (Look x #t #f)))]
         [(eat! #\>) (note! "atomic group") (finish Atomic)]
         [(eat-str! "<=") (note! "lookbehind is treated as always true")
                          (finish (lambda (x) (Look x #f #t)))]
         [(eat-str! "<!") (note! "lookbehind is treated as always true")
                          (finish (lambda (x) (Look x #t #t)))]
         [(or (eat-str! "P<") (eat-str! "<"))
          (let ([idx (capture! (read-name!))]) (finish (lambda (x) (Grp x idx))))]
         [else (rx-error "unsupported group syntax at offset ~a" start)])]
      [else
       (let ([idx (capture! #f)]) (finish (lambda (x) (Grp x idx))))]))

  (define (parse-escape start)
    (when (>= pos n) (rx-error "pattern ends with a backslash"))
    (define c (peek))
    (cond
      [(class-escape c) => (lambda (s) (advance!) (leaf s start))]
      [(eqv? c #\b) (advance!) (note! "word boundaries are treated as empty") (WordB #f)]
      [(eqv? c #\B) (advance!) (note! "word boundaries are treated as empty") (WordB #t)]
      [(memv c '(#\p #\P)) (skip-property!) (leaf ALL start)]
      [(and (char>=? c #\1) (char<=? c #\9))
       (define m (regexp-match #px"^[0-9]+" src pos))
       (set! pos (+ pos (string-length (car m))))
       (define k (string->number (car m)))
       (set! max-backref (max max-backref k))
       (note! "backreferences are not modelled by the static analysis")
       (Backref k)]
      [(and (eqv? c #\k) (eqv? (peek-at 1) #\<))
       (advance!) (advance!)
       (define name (read-name!))
       (define idx (hash-ref names name #f))
       (unless idx (rx-error "unknown group name ~a" name))
       (note! "backreferences are not modelled by the static analysis")
       (Backref idx)]
      [else (leaf (single (read-escape-char)) start)]))

  (define (parse-class start)
    (define neg? (eat! #\^))
    (define (class-atom)
      (define c (advance!))
      (cond
        [(eqv? c #\\)
         (when (>= pos n) (rx-error "pattern ends with a backslash"))
         (define e (peek))
         (cond [(class-escape e) => (lambda (s) (advance!) s)]
               [(eqv? e #\b) (advance!) 8]
               [(memv e '(#\p #\P)) (skip-property!) ALL]
               [else (read-escape-char)])]
        [else (char->integer c)]))
    (define (as-set x) (if (number? x) (single x) x))
    (let loop ([acc '()])
      (cond
        [(>= pos n) (rx-error "unterminated character class at offset ~a" start)]
        [(eqv? (peek) #\])
         (advance!)
         (let* ([s (norm-set acc)]
                [s (if icase? (case-fold-set s) s)]
                [s (if neg? (set-complement s) s)])
           (Chars s (cons start pos)))]
        [else
         (define a (class-atom))
         (cond
           [(and (eqv? (peek) #\-) (peek-at 1) (not (eqv? (peek-at 1) #\])))
            (advance!)
            (define b (class-atom))
            (cond
              [(and (number? a) (number? b))
               (when (> a b) (rx-error "class range out of order at offset ~a" start))
               (loop (cons (cons a b) acc))]
              [else (loop (append (as-set a) (as-set b) (single 45) acc))])]
           [else (loop (append (as-set a) acc))])])))

  (define ast (parse-alt))
  (when (< pos n) (rx-error "unmatched ) at offset ~a" pos))
  (when (> max-backref ngroups) (rx-error "backreference \\~a has no matching group" max-backref))
  (values ast ngroups (reverse notes)))

;; ---------------------------------------------------------------------------
;; Reference backtracking matcher. Same search order as Perl, PCRE, Java, Python
;; and V8 without its experimental linear engine. No memoisation on purpose.

(define (run-match ast ngroups str search? budget)
  (define len (string-length str))
  (define steps 0)
  (define caps (make-vector (* 2 (add1 ngroups)) #f))
  (define (word-at? i)
    (and (>= i 0) (< i len) (set-member? WORD (char->integer (string-ref str i)))))

  (define (m node i k)
    (set! steps (add1 steps))
    (when (> steps budget) (raise 'budget))
    (cond
      [(Chars? node)
       (and (< i len)
            (set-member? (Chars-set node) (char->integer (string-ref str i)))
            (k (add1 i)))]
      [(Cat? node)
       (let loop ([items (Cat-items node)] [i i])
         (if (null? items)
             (k i)
             (m (car items) i (lambda (j) (loop (cdr items) j)))))]
      [(Alt? node)
       (for/or ([a (in-list (Alt-items node))]) (m a i k))]
      [(Grp? node)
       (define idx (Grp-idx node))
       (m (Grp-node node) i
          (lambda (j)
            (define old-s (vector-ref caps (* 2 idx)))
            (define old-e (vector-ref caps (add1 (* 2 idx))))
            (vector-set! caps (* 2 idx) i)
            (vector-set! caps (add1 (* 2 idx)) j)
            (or (k j)
                (begin (vector-set! caps (* 2 idx) old-s)
                       (vector-set! caps (add1 (* 2 idx)) old-e)
                       #f))))]
      [(Rep? node)
       (define sub (Rep-node node))
       (define mn (Rep-min node))
       (define mx (Rep-max node))
       (let loop ([count 0] [i i])
         (define (more)
           (and (or (not mx) (< count mx))
                (m sub i (lambda (j)
                           (and (or (> j i) (< count mn))
                                (loop (add1 count) j))))))
         (define (stop) (and (>= count mn) (k i)))
         (if (Rep-greedy node)
             (or (more) (stop))
             (or (stop) (more))))]
      [(Atomic? node)
       (define end (m (Atomic-node node) i (lambda (j) j)))
       (and end (k end))]
      [(Look? node)
       (cond [(Look-behind? node) (k i)]
             [else
              (define r (m (Look-node node) i (lambda (j) #t)))
              (if (Look-neg? node)
                  (and (not r) (k i))
                  (and r (k i)))])]
      [(Backref? node)
       (define idx (Backref-n node))
       (define s (vector-ref caps (* 2 idx)))
       (define e (vector-ref caps (add1 (* 2 idx))))
       (cond [(not s) (k i)]
             [else
              (define l (- e s))
              (and (<= (+ i l) len)
                   (string=? (substring str s e) (substring str i (+ i l)))
                   (k (+ i l)))])]
      [(Bol? node) (and (= i 0) (k i))]
      [(Eol? node) (and (= i len) (k i))]
      [(WordB? node)
       (define b? (not (eq? (word-at? (sub1 i)) (word-at? i))))
       (and (if (WordB-neg? node) (not b?) b?) (k i))]
      [else (k i)]))

  (with-handlers ([(lambda (e) (eq? e 'budget)) (lambda (_) (values #f steps #t))])
    (let ()
      (define ok?
        (for/or ([start (in-range 0 (if search? (add1 len) 1))])
          (vector-fill! caps #f)
          (and (m ast start (lambda (j) #t)) #t)))
      (values ok? steps #f))))

;; ---------------------------------------------------------------------------
;; Glushkov position automaton. One position per character class leaf, no
;; epsilon edges, so two accepting paths on one word are two ways to match.

(struct nfa (n labels follow finals spans live) #:transparent)

(define (start-anchored? node)
  (cond [(Bol? node) #t]
        [(Cat? node)
         (define items (filter (lambda (x) (not (Empty? x))) (Cat-items node)))
         (and (pair? items) (start-anchored? (car items)))]
        [(Alt? node) (andmap start-anchored? (Alt-items node))]
        [(Grp? node) (start-anchored? (Grp-node node))]
        [(Atomic? node) (start-anchored? (Atomic-node node))]
        [else #f]))

(define (build-nfa ast search? max-positions)
  (define labels (make-hasheqv))
  (define spans (make-hasheqv))
  (define follow (make-hasheqv))
  (define count 0)

  (define alias (make-hasheqv))
  (define (new-pos! set span)
    (set! count (add1 count))
    (when (> count max-positions) (raise 'too-many-positions))
    (hash-set! labels count set)
    (hash-set! spans count span)
    count)
  ;; A second structural edge between the same two positions is a second way
  ;; to match, so it gets its own copy of the target. Without this the
  ;; automaton merges the two parses of (a+)+ and hides the blowup.
  (define (clone! t)
    (define c (new-pos! (hash-ref labels t) (hash-ref spans t)))
    (hash-set! alias c t)
    c)
  (define (add-follow! froms tos)
    (unless (null? tos)
      (for ([f (in-list froms)])
        (hash-update! follow f
                      (lambda (existing)
                        (for/fold ([acc existing]) ([t (in-list tos)])
                          (cons (if (memv t acc) (clone! t) t) acc)))
                      '()))))

  (define (g-seq thunks)
    (for/fold ([nl #t] [fst '()] [lst '()]) ([th (in-list thunks)])
      (define-values (inl ifst ilst) (th))
      (add-follow! lst ifst)
      (values (and nl inl)
              (if nl (append fst ifst) fst)
              (if inl (append lst ilst) ilst))))

  (define (g-star sub)
    (define-values (nl f l) (g sub))
    (add-follow! l f)
    (values #t f l))

  (define (g-opt-chain sub k)
    (cond [(zero? k) (values #t '() '())]
          [else
           (define-values (nl f l)
             (g-seq (list (lambda () (g sub))
                          (lambda () (g-opt-chain sub (sub1 k))))))
           (values #t f l)]))

  (define (g-rep sub mn mx)
    (cond
      [(and (= mn 0) (not mx)) (g-star sub)]
      [(and (= mn 1) (not mx))
       (define-values (nl f l) (g sub))
       (add-follow! l f)
       (values nl f l)]
      [(and (= mn 0) (eqv? mx 1))
       (define-values (nl f l) (g sub))
       (values #t f l)]
      [else
       (g-seq (append
               (for/list ([_ (in-range mn)]) (lambda () (g sub)))
               (cond [(not mx) (list (lambda () (g-star sub)))]
                     [(> mx mn) (list (lambda () (g-opt-chain sub (- mx mn))))]
                     [else '()])))]))

  (define (g node)
    (cond
      [(Chars? node)
       (cond [(null? (Chars-set node)) (values #f '() '())]
             [else (define p (new-pos! (Chars-set node) (Chars-span node)))
                   (when (> count max-positions) (raise 'too-many-positions))
                   (values #f (list p) (list p))])]
      [(Cat? node) (g-seq (for/list ([x (in-list (Cat-items node))]) (lambda () (g x))))]
      [(Alt? node)
       (for/fold ([nl #f] [fst '()] [lst '()]) ([x (in-list (Alt-items node))])
         (define-values (inl ifst ilst) (g x))
         (values (or nl inl) (append fst ifst) (append lst ilst)))]
      [(Rep? node) (g-rep (Rep-node node) (Rep-min node) (Rep-max node))]
      [(Grp? node) (g (Grp-node node))]
      [(Atomic? node) (g (Atomic-node node))]
      [else (values #t '() '())]))

  (define-values (root-nl root-first root-last) (g ast))
  (define any-pos (and search? (new-pos! ALL #f)))
  (define init-follow (if any-pos (cons any-pos root-first) root-first))
  (when any-pos
    (hash-set! follow any-pos (cons any-pos root-first)))
  (hash-set! follow 0 init-follow)
  (define n count)
  (define finals
    (append root-last
            (if root-nl (if any-pos (list 0 any-pos) (list 0)) '())))

  (define (root p) (hash-ref alias p p))
  (define follow-vec
    (for/vector #:length (add1 n) ([p (in-range (add1 n))])
      (sort (remove-duplicates (hash-ref follow (root p) '())) <)))
  (define final-vec (make-vector (add1 n) #f))
  (for ([f (in-list finals)]) (vector-set! final-vec f #t))
  (for ([p (in-range 1 (add1 n))])
    (when (vector-ref final-vec (root p)) (vector-set! final-vec p #t)))

  ;; trim to states on some path from the start to a final state
  (define reach (make-vector (add1 n) #f))
  (let loop ([stack '(0)])
    (unless (null? stack)
      (define s (car stack))
      (cond [(vector-ref reach s) (loop (cdr stack))]
            [else (vector-set! reach s #t)
                  (loop (append (vector-ref follow-vec s) (cdr stack)))])))
  (define preds (make-vector (add1 n) '()))
  (for* ([p (in-range (add1 n))] [q (in-list (vector-ref follow-vec p))])
    (vector-set! preds q (cons p (vector-ref preds q))))
  (define coreach (make-vector (add1 n) #f))
  (let loop ([stack (filter (lambda (s) (vector-ref final-vec s)) (range (add1 n)))])
    (unless (null? stack)
      (define s (car stack))
      (cond [(vector-ref coreach s) (loop (cdr stack))]
            [else (vector-set! coreach s #t)
                  (loop (append (vector-ref preds s) (cdr stack)))])))
  (define live (for/vector #:length (add1 n) ([s (in-range (add1 n))])
                 (and (vector-ref reach s) (vector-ref coreach s))))
  (define trimmed
    (for/vector #:length (add1 n) ([s (in-range (add1 n))])
      (if (vector-ref live s)
          (filter (lambda (t) (vector-ref live t)) (vector-ref follow-vec s))
          '())))
  (nfa n
       (for/vector #:length (add1 n) ([p (in-range (add1 n))]) (hash-ref labels p '()))
       trimmed
       final-vec
       (for/vector #:length (add1 n) ([p (in-range (add1 n))]) (hash-ref spans p #f))
       live))

(define (live? A s) (vector-ref (nfa-live A) s))

;; ---------------------------------------------------------------------------
;; Graph helpers

(define (tarjan roots succs size on-scc)
  (define index (make-vector size #f))
  (define low (make-vector size 0))
  (define on (make-vector size #f))
  (define stack '())
  (define counter 0)
  (define (visit v)
    (vector-set! index v counter)
    (vector-set! low v counter)
    (set! counter (add1 counter))
    (set! stack (cons v stack))
    (vector-set! on v #t)
    (for ([w (in-list (succs v))])
      (cond [(not (vector-ref index w))
             (visit w)
             (vector-set! low v (min (vector-ref low v) (vector-ref low w)))]
            [(vector-ref on w)
             (vector-set! low v (min (vector-ref low v) (vector-ref index w)))]))
    (when (= (vector-ref low v) (vector-ref index v))
      (let loop ([members '()])
        (define w (car stack))
        (set! stack (cdr stack))
        (vector-set! on w #f)
        (if (= w v)
            (on-scc (cons w members))
            (loop (cons w members))))))
  (for ([r (in-list roots)])
    (unless (vector-ref index r) (visit r))))

;; BFS returning the node path from start to the first node satisfying goal?
(define (bfs-path start goal? succs allowed? #:limit [limit #f])
  (define prev (make-hasheqv))
  (hash-set! prev start #f)
  (define seen 0)
  (let loop ([queue (list start)] [next '()])
    (cond
      [(and (null? queue) (null? next)) #f]
      [(null? queue) (loop (reverse next) '())]
      [else
       (define v (car queue))
       (set! seen (add1 seen))
       (cond
         [(and limit (> seen limit)) 'limit]
         [else
          (define fresh
            (for/list ([w (in-list (succs v))]
                       #:when (and (allowed? w) (not (hash-has-key? prev w))))
              (hash-set! prev w v)
              w))
          (define hit (findf goal? fresh))
          (cond
            [hit
             (let build ([x hit] [acc '()])
               (if x (build (hash-ref prev x) (cons x acc)) acc))]
            [else (loop (cdr queue) (append (reverse fresh) next))])])])))

(define (shortest-word A target)
  (cond
    [(= target 0) ""]
    [else
     (define path
       (bfs-path 0 (lambda (s) (= s target))
                 (lambda (s) (vector-ref (nfa-follow A) s))
                 (lambda (s) #t)))
     (and (pair? path)
          (list->string
           (for/list ([s (in-list (cdr path))])
             (pick-char (vector-ref (nfa-labels A) s)))))]))

(define (make-pair-set A)
  (define N1 (add1 (nfa-n A)))
  (define cache (make-hasheqv))
  (lambda (p r)
    (define key (+ (* p N1) r))
    (hash-ref! cache key
               (lambda ()
                 (set-intersect (vector-ref (nfa-labels A) p)
                                (vector-ref (nfa-labels A) r))))))

;; ---------------------------------------------------------------------------
;; Exponential ambiguity: a state q and a word w with two different paths
;; q to q. In the pair graph that is an SCC holding both a diagonal node and an
;; off diagonal node.

(define (find-eda A)
  (define N1 (add1 (nfa-n A)))
  (define follow (nfa-follow A))
  (define pair-set (make-pair-set A))
  (define (succs v)
    (define p (quotient v N1))
    (define r (remainder v N1))
    (for*/list ([p2 (in-list (vector-ref follow p))]
                [r2 (in-list (vector-ref follow r))]
                #:when (pair? (pair-set p2 r2)))
      (+ (* p2 N1) r2)))
  (define (diag? v) (= (quotient v N1) (remainder v N1)))
  (define roots
    (for/list ([q (in-range 1 N1)] #:when (live? A q)) (+ (* q N1) q)))
  (let/ec return
    (tarjan roots succs (* N1 N1)
            (lambda (members)
              (when (pair? (cdr members))
                (define diag (filter diag? members))
                (when (and (pair? diag) (< (length diag) (length members)))
                  (return (eda-witness A members diag N1 succs pair-set))))))
    #f))

(define (eda-witness A members diag N1 succs pair-set)
  (define in-scc (for/hasheqv ([v (in-list members)]) (values v #t)))
  (define (allowed? v) (hash-has-key? in-scc v))
  (define q (apply min (map (lambda (v) (quotient v N1)) diag)))
  (define start (+ (* q N1) q))
  (define out (bfs-path start (lambda (v) (not (= (quotient v N1) (remainder v N1))))
                        succs allowed?))
  (define off (last out))
  (define back (bfs-path off (lambda (v) (= v start)) succs allowed?))
  (define path (append out (cdr back)))
  (define pump
    (list->string
     (for/list ([a (in-list path)] [b (in-list (cdr path))])
       (pick-char (pair-set (quotient b N1) (remainder b N1))))))
  (define states
    (remove-duplicates
     (append* (for/list ([v (in-list path)])
                (list (quotient v N1) (remainder v N1))))))
  (list (shortest-word A q) pump states))

;; ---------------------------------------------------------------------------
;; Polynomial ambiguity: states p != q and a word w with p to p, p to q and
;; q to q. That is a path from (p,p,q) to (p,q,q) in the triple product.

(define (state-reach A)
  (define n1 (add1 (nfa-n A)))
  (define follow (nfa-follow A))
  (for/vector #:length n1 ([s (in-range n1)])
    (define seen (make-hasheqv))
    (let loop ([stack (vector-ref follow s)])
      (unless (null? stack)
        (define t (car stack))
        (cond [(hash-has-key? seen t) (loop (cdr stack))]
              [else (hash-set! seen t #t)
                    (loop (append (vector-ref follow t) (cdr stack)))])))
    seen))

(define (find-ida A budget max-found)
  (define N1 (add1 (nfa-n A)))
  (define follow (nfa-follow A))
  (define labels (nfa-labels A))
  (define pair-set (make-pair-set A))
  (define reach (state-reach A))
  (define (cyclic? s) (hash-has-key? (vector-ref reach s) s))
  (define (tid a b c) (+ (* (+ (* a N1) b) N1) c))
  (define (parts v)
    (values (quotient v (* N1 N1)) (remainder (quotient v N1) N1) (remainder v N1)))
  (define (triple-set a b c) (set-intersect (pair-set a b) (vector-ref labels c)))
  (define spent 0)
  (define found '())
  (define truncated? #f)
  (let/ec done
    (for* ([p (in-range 1 N1)]
           #:when (and (live? A p) (cyclic? p))
           [q (in-range 1 N1)]
           #:when (and (not (= p q)) (live? A q) (cyclic? q)
                       (hash-has-key? (vector-ref reach p) q)))
      (define target (tid p q q))
      (define (succs v)
        (define-values (a b c) (parts v))
        (for*/list ([a2 (in-list (vector-ref follow a))]
                    [b2 (in-list (vector-ref follow b))]
                    [c2 (in-list (vector-ref follow c))]
                    #:when (pair? (triple-set a2 b2 c2)))
          (tid a2 b2 c2)))
      (define path (bfs-path (tid p p q) (lambda (v) (= v target)) succs
                             (lambda (v) #t) #:limit (- budget spent)))
      (cond
        [(eq? path 'limit) (set! truncated? #t) (done (void))]
        [(pair? path)
         (set! spent (+ spent (length path)))
         (define pump
           (list->string
            (for/list ([x (in-list path)] [y (in-list (cdr path))])
              (define-values (a b c) (parts y))
              (pick-char (triple-set a b c)))))
         (define prefix (shortest-word A p))
         (set! found (cons (list prefix pump (list p q)) found))
         (when (>= (length found) max-found) (done (void)))]
        [else (void)])))
  (values (reverse found) truncated?))

;; When the automaton proves nothing, single character floods still catch growth
;; that is capped by counted repeats, such as (a?){25}a{25} or (a|a){1,60}.
(define (probe-witnesses A)
  (define picks
    (remove-duplicates
     (for/list ([s (in-range 1 (add1 (nfa-n A)))]
                #:when (and (live? A s) (not (equal? (vector-ref (nfa-labels A) s) ALL))))
       (pick-char (vector-ref (nfa-labels A) s)))))
  (for/list ([c (in-list (if (> (length picks) 8) (take picks 8) picks))])
    (list "" (string c) '())))

;; ---------------------------------------------------------------------------
;; Measuring a witness against the reference matcher

(define SCHEDULE (append (range 1 17) '(32 64 128 256 512)))

(define (repeat-string s k) (string-append* (for/list ([_ (in-range k)]) s)))

(define (outside-chars A)
  (define used
    (for/fold ([u '()]) ([s (in-range 1 (add1 (nfa-n A)))])
      (define l (vector-ref (nfa-labels A) s))
      (if (equal? l ALL) u (set-union u l))))
  (define free (set-complement used))
  (remove-duplicates
   (append
    (for/list ([c (in-string "!~@#$%^&*|<>")]
               #:when (set-member? free (char->integer c)))
      (string c))
    (list "\n" "\u0001" "!" "" "a" "0" " " "-"))))

(define (measure-witness ast ngroups search? witness suffixes o)
  (define prefix (first witness))
  (define pump (second witness))
  (define (attack k suffix) (string-append prefix (repeat-string pump k) suffix))
  (define (steps-of str budget)
    (define-values (ok? steps ex?) (run-match ast ngroups str search? budget))
    steps)
  (define suffix
    (let ([scored (for/list ([s (in-list suffixes)])
                    (cons (steps-of (attack 10 s) 200000) s))])
      (cdr (argmax car scored))))
  (define series
    (let loop ([ks SCHEDULE] [acc '()])
      (cond
        [(null? ks) (reverse acc)]
        [else
         (define str (attack (car ks) suffix))
         (cond
           [(> (string-length str) (opts-max-attack o)) (reverse acc)]
           [else
            (define-values (ok? steps ex?)
              (run-match ast ngroups str search? (opts-max-steps o)))
            (define row (list (car ks) (string-length str) steps ex? ok?))
            (if ex? (reverse (cons row acc)) (loop (cdr ks) (cons row acc)))])])))
  (list prefix pump suffix series))

(define (row-k r) (first r))
(define (row-len r) (second r))
(define (row-steps r) (third r))
(define (row-ex? r) (fourth r))

;; -> (list kind degree base) with kind in exponential polynomial linear inconclusive
(define (classify series budget)
  (define (st k)
    (define e (findf (lambda (r) (and (= (row-k r) k) (not (row-ex? r)))) series))
    (and e (max 1 (row-steps e))))
  (define ex (for/first ([r (in-list series)] #:when (row-ex? r)) (row-k r)))
  (define (log2 x) (/ (log x) (log 2)))
  (define (growth-base)
    (define ok (filter (lambda (r) (and (not (row-ex? r)) (>= (row-steps r) 50) (<= (row-k r) 16)))
                       series))
    (and (>= (length ok) 2)
         (let* ([b (last ok)] [a (list-ref ok (- (length ok) 2))])
           (and (> (row-k b) (row-k a))
                (/ (round (* 100 (expt (/ (row-steps b) (row-steps a))
                                       (/ 1.0 (- (row-k b) (row-k a))))))
                   100.0)))))
  (define (len-at k) (row-len (findf (lambda (r) (= (row-k r) k)) series)))
  ;; slopes are taken against input length so the pump size does not matter
  (define (lslope lo hi-steps)
    (log2-ratio hi-steps (st lo) (/ (len-at (* 2 lo)) (len-at lo))))
  (define (log2-ratio hi lo len-ratio)
    (/ (log (/ hi lo)) (log len-ratio)))
  (define measured-points
    (for*/list ([lo (in-list '(8 16 32 64 128 256))]
                #:when (and (st lo) (st (* 2 lo))))
      (lslope lo (st (* 2 lo)))))
  (define bound-points
    (for*/list ([lo (in-list '(8 16 32 64 128 256))]
                #:when (and (st lo) ex (= ex (* 2 lo))))
      (lslope lo budget)))
  (define points (if (null? measured-points) bound-points measured-points))
  (define s (and (pair? points) (last points)))
  (define accelerating?
    (and (>= (length points) 2)
         (let ([a (list-ref points (- (length points) 2))] [b (last points)])
           (and (>= b (* 1.6 a)) (>= (- b a) 1.0)))))
  (define floor-degree
    (and ex (> ex 1) (> (len-at ex) 1) (/ (log budget) (log (len-at ex)))))
  (cond
    [(and (not ex) (null? points)) (list 'inconclusive #f #f #f)]
    [(and (not ex) (< s 1.35)) (list 'linear 1 #f #f)]
    [else
     (list 'grows
           (cond [(and s floor-degree) (max s floor-degree)] [else (or s floor-degree)])
           (growth-base)
           (or (and ex (<= ex 32)) accelerating?)
           floor-degree)]))

;; growth class from the measurement plus what the automaton proved:
;; (list kind degree base lower-bound?)
(define (final-class gc eda? series [probe? #f])
  (define exceeded? (ormap row-ex? series))
  (case (first gc)
    [(linear) (list 'linear 1 #f #f)]
    [(inconclusive) (list 'inconclusive #f #f #f)]
    [else
     (define degree (max 2 (exact-round* (+ (or (second gc) 2) 0.2))))
     (cond [probe?
            (if (or exceeded? (>= (or (second gc) 0) 2.5))
                (list 'bounded #f #f #t)
                (list 'linear 1 #f #f))]
           [(and eda? (fourth gc)) (list 'exponential #f (third gc) #f)]
           [exceeded?
            ;; the budget ran out, so only a conservative floor is honest
            (list 'polynomial
                  (max 3 (min degree (exact-round* (ceiling (or (fifth gc) degree)))))
                  #f #t)]
           [else (list 'polynomial degree #f #f)])]))

(define (exact-round* x) (inexact->exact (round x)))

(define (severity c)
  (case (first c)
    [(exponential) 1000]
    [(polynomial) (second c)]
    [(bounded) 500]
    [else 0]))

(define (extrapolate-length c measured witness)
  (define series (fourth measured))
  (define ok (filter (lambda (r) (and (not (row-ex? r)) (> (row-steps r) 100))) series))
  (cond
    [(null? ok) #f]
    [else
     (define r (last ok))
     (define pump-len (string-length (second measured)))
     (case (first c)
       [(exponential)
        (define b (third c))
        (and b (> b 1.0)
             (let ([k (+ (row-k r) (/ (log (/ STEP-TARGET (row-steps r))) (log b)))])
               (exact-round* (+ (string-length (first measured))
                                (* k pump-len)
                                (string-length (third measured))))))]
       [(polynomial)
        (exact-round* (* (row-len r) (expt (/ STEP-TARGET (row-steps r)) (/ 1.0 (second c)))))]
       [else #f])]))

;; ---------------------------------------------------------------------------
;; Analysis driver

(define (span-text pattern span)
  (and span (<= (cdr span) (string-length pattern))
       (substring pattern (car span) (cdr span))))

(define (loci A states pattern)
  (remove-duplicates
   (for*/list ([s (in-list states)]
               [sp (in-value (vector-ref (nfa-spans A) s))]
               #:when sp)
     (list (car sp) (cdr sp) (span-text pattern sp)))))

(define (flag-set flags)
  (for/list ([c (in-string flags)]) c))

(define (analyze-pattern pattern flags #:id [id #f] #:opts [o DEFAULT-OPTS])
  (define fl (flag-set flags))
  (define base
    (hasheq 'type "pattern" 'id (or id 'null) 'pattern pattern 'flags flags))
  (define (finish status . kvs)
    (let loop ([h (hash-set base 'status status)] [kvs kvs])
      (if (null? kvs) h (loop (hash-set h (car kvs) (cadr kvs)) (cddr kvs)))))
  (with-handlers ([exn:regex? (lambda (e) (finish "error" 'message (exn-message e)))]
                  [(lambda (e) (eq? e 'too-many-positions))
                   (lambda (_) (finish "unanalyzed"
                                       'message (format "more than ~a positions after expanding counted repeats"
                                                        (opts-max-positions o))))])
    (define-values (ast ngroups notes)
      (parse-regex pattern #:icase? (and (memv #\i fl) #t) #:dotall? (and (memv #\s fl) #t)))
    (define notes*
      (if (memv #\m fl)
          (append notes (list "multiline flag ignored, ^ and $ are the input boundaries"))
          notes))
    (define search? (and (opts-search? o) (not (start-anchored? ast))))
    (define A (build-nfa ast search? (opts-max-positions o)))
    (define n (nfa-n A))
    (define eda (find-eda A))
    (define-values (static-witnesses truncated?)
      (cond [eda (values (list eda) #f)]
            [(<= n 80) (find-ida A (opts-ida-budget o) (opts-max-witnesses o))]
            [else (values '() 'skipped)]))
    (define probe? (null? static-witnesses))
    (define witnesses
      (if probe? (probe-witnesses A) static-witnesses))
    (define caveats
      (append notes*
              (cond [(eq? truncated? 'skipped)
                     (list (format "polynomial search skipped: ~a positions is over the limit of 80" n))]
                    [truncated? (list "polynomial search stopped at its node budget")]
                    [else '()])))
    (cond
      [(null? witnesses)
       (finish "safe" 'positions n 'search_mode search? 'caveats caveats)]
      [else
       (define suffixes (outside-chars A))
       (define measured
         (for/list ([w (in-list witnesses)])
           (define m (measure-witness ast ngroups search? w suffixes o))
           (define c (final-class (classify (fourth m) (opts-max-steps o)) (and eda #t) (fourth m) probe?))
           (list m c w)))
       (define best
         (argmax (lambda (x)
                   (+ (* 1000000 (severity (second x)))
                      (let ([s (fourth (first x))])
                        (if (null? s) 0 (min 999999 (row-steps (last s)))))))
                 measured))
       (define m (first best))
       (define c (second best))
       (define w (third best))
       (define series (fourth m))
       (define kind (first c))
       (define degree (second c))
       (define vulnerable? (or (eq? kind 'exponential) (eq? kind 'bounded)
                               (and (eq? kind 'polynomial) degree (>= degree 2))))
       (define states (third w))
       (define details
         (list 'positions n
               'search_mode search?
               'caveats caveats
               'locus (map (lambda (l) (hasheq 'start (first l) 'end (second l) 'text (third l)))
                           (loci A states pattern))
               'witness (hasheq 'prefix (first m) 'pump (second m) 'suffix (third m))
               'measured (for/list ([r (in-list series)])
                           (hasheq 'k (row-k r) 'length (row-len r) 'steps (row-steps r)
                                   'exceeded (row-ex? r)))))
       (define (with-details status . extra)
         (apply finish status (append extra details)))
       (cond
         [vulnerable?
          (with-details "vulnerable"
            'verdict (symbol->string kind)
            'degree (or degree 'null)
            'degree_is_lower_bound (fourth c)
            'base (or (third c) 'null)
            'attack_length_for_100m_steps (or (extrapolate-length c m w) 'null))]
         [probe? (finish "safe" 'positions n 'search_mode search? 'caveats caveats)]
         [else
          (with-details "suspect"
            'verdict (symbol->string kind)
            'degree 'null
            'base 'null
            'attack_length_for_100m_steps 'null)])])))

;; ---------------------------------------------------------------------------
;; Input handling

(define (parse-input-line line lineno)
  (define t (string-trim line))
  (cond
    [(string=? t "") #f]
    [(char=? (string-ref t 0) #\#) #f]
    [(char=? (string-ref t 0) #\{)
     (define j (with-handlers ([exn:fail? (lambda (e) (raise (exn:regex "line is not valid JSON" (current-continuation-marks))))])
                 (string->jsexpr t)))
     (unless (and (hash? j) (string? (hash-ref j 'pattern #f)))
       (raise (exn:regex "JSON line needs a string field named pattern" (current-continuation-marks))))
     (list (let ([i (hash-ref j 'id #f)]) (if (or (string? i) (number? i)) i lineno))
           (hash-ref j 'pattern)
           (let ([f (hash-ref j 'flags "")]) (if (string? f) f "")))]
    [(regexp-match #px"^/(.+)/([a-z]*)$" t)
     => (lambda (m)
          (if (regexp-match? #px"^[dgimsuvy]*$" (caddr m))
              (list lineno (cadr m) (caddr m))
              (list lineno t "")))]
    [else (list lineno line "")]))

(define (analyze-lines lines extra-flags o)
  (for*/list ([(line idx) (in-indexed (in-list lines))]
              [parsed (in-value
                       (with-handlers ([exn:regex? (lambda (e) e)])
                         (parse-input-line line (add1 idx))))]
              #:when parsed)
    (cond
      [(exn? parsed)
       (hasheq 'type "pattern" 'id (add1 idx) 'pattern line 'flags ""
               'status "error" 'message (exn-message parsed))]
      [else
       (analyze-pattern (second parsed)
                        (string-append (third parsed) extra-flags)
                        #:id (first parsed) #:opts o)])))

;; ---------------------------------------------------------------------------
;; Output

(define (show-string s)
  (string-append
   "\""
   (string-append*
    (for/list ([c (in-string s)])
      (define cp (char->integer c))
      (cond [(char=? c #\\) "\\\\"]
            [(char=? c #\") "\\\""]
            [(char=? c #\newline) "\\n"]
            [(char=? c #\return) "\\r"]
            [(char=? c #\tab) "\\t"]
            [(<= 32 cp 126) (string c)]
            [else (format "\\u{~a}" (number->string cp 16))])))
   "\""))

(define (fmt-steps n)
  (cond [(>= n 1000000) (format "~aM" (/ (round (/ n 100000.0)) 10))]
        [(>= n 1000) (format "~ak" (/ (round (/ n 100.0)) 10))]
        [else (number->string n)]))

(define HINT-EXP
  "Two ways of matching the same text sit inside a repeat. Make the alternatives disjoint, drop the inner quantifier, use an atomic group or possessive quantifier if the engine has them, or run the pattern on a linear engine such as RE2, Rust regex or Go regexp.")
(define HINT-BOUNDED
  "Counted repeats cap the growth, but the cap is far beyond what a request should cost. Lower the bounds, make the repeated alternatives disjoint, or reject long input before matching.")
(define HINT-POLY
  "Neighbouring parts compete for the same characters. Anchor the pattern, make the parts disjoint, split the work into two passes, or cap the input length.")

(define (render-text r)
  (define status (hash-ref r 'status))
  (define head
    (format "~a  ~a  ~a"
            (cond [(and (string=? status "vulnerable") (equal? (hash-ref r 'verdict) "bounded"))
                   "VULNERABLE bounded explosion"]
                  [(string=? status "vulnerable")
                   (if (equal? (hash-ref r 'verdict) "exponential")
                       "VULNERABLE exponential"
                       (format "VULNERABLE polynomial degree ~a~a"
                                      (if (hash-ref r 'degree_is_lower_bound #f) "at least " "")
                                      (hash-ref r 'degree)))]
                  [(string=? status "suspect") "SUSPECT"]
                  [(string=? status "safe") "SAFE"]
                  [(string=? status "unanalyzed") "UNANALYZED"]
                  [else "ERROR"])
            (let ([id (hash-ref r 'id)]) (if (eq? id 'null) "-" id))
            (show-string (hash-ref r 'pattern))))
  (define lines
    (cond
      [(member status '("error" "unanalyzed"))
       (list (format "  ~a" (hash-ref r 'message)))]
      [(string=? status "safe")
       (for/list ([c (in-list (hash-ref r 'caveats '()))]) (format "  note: ~a" c))]
      [else
       (define w (hash-ref r 'witness))
       (define series (hash-ref r 'measured))
       (append
        (list (format "  attack: ~a + ~a x k + ~a"
                      (show-string (hash-ref w 'prefix))
                      (show-string (hash-ref w 'pump))
                      (show-string (hash-ref w 'suffix))))
        (list (format "  steps: ~a"
                      (string-join
                       (for/list ([m (in-list series)]
                                  #:when (memv (hash-ref m 'k) '(1 2 4 8 16 32 64 128 256 512)))
                         (format "k=~a ~a~a" (hash-ref m 'k)
                                 (if (hash-ref m 'exceeded) ">" "")
                                 (fmt-steps (hash-ref m 'steps))))
                       "  ")))
        (let ([ex (findf (lambda (m) (hash-ref m 'exceeded)) series)])
          (if ex
              (list (format "  budget exceeded at k=~a (input length ~a)" (hash-ref ex 'k) (hash-ref ex 'length)))
              '()))
        (let ([loc (hash-ref r 'locus '())])
          (if (null? loc) '()
              (list (format "  where: ~a"
                            (string-join (for/list ([l (in-list loc)])
                                           (format "[~a,~a) ~a" (hash-ref l 'start) (hash-ref l 'end)
                                                   (show-string (hash-ref l 'text))))
                                         "  ")))))
        (let ([len (hash-ref r 'attack_length_for_100m_steps 'null)])
          (if (eq? len 'null) '()
              (list (format "  about 100M steps at ~a characters of hostile input" len))))
        (if (string=? status "vulnerable")
            (list (format "  fix: ~a"
                          (case (hash-ref r 'verdict)
                            [("exponential") HINT-EXP]
                            [("bounded") HINT-BOUNDED]
                            [else HINT-POLY])))
            (list "  not reproduced: the reference matcher stayed linear on this input, usually because a match is found before backtracking starts"))
        (for/list ([c (in-list (hash-ref r 'caveats '()))]) (format "  note: ~a" c)))]))
  (string-join (cons head lines) "\n"))

(define (level-hit? r level)
  (define status (hash-ref r 'status))
  (define vuln? (string=? status "vulnerable"))
  (define exp? (and vuln? (equal? (hash-ref r 'verdict) "exponential")))
  (define deg (let ([d (hash-ref r 'degree 'null)]) (if (number? d) d 0)))
  (cond [(string=? level "none") #f]
        [(string=? level "exponential") exp?]
        [(string=? level "cubic")
         (or exp? (and vuln? (>= deg 3)) (and vuln? (equal? (hash-ref r 'verdict) "bounded")))]
        [(string=? level "quadratic") vuln?]
        [(string=? level "suspect") (or vuln? (string=? status "suspect"))]
        [else #f]))

(define (exit-code-for results level)
  (cond [(ormap (lambda (r) (level-hit? r level)) results) 1]
        [(ormap (lambda (r) (member (hash-ref r 'status) '("error" "unanalyzed"))) results) 3]
        [else 0]))

(define (summary results)
  (define (count s) (length (filter (lambda (r) (string=? (hash-ref r 'status) s)) results)))
  (hasheq 'type "summary" 'patterns (length results)
          'vulnerable (count "vulnerable") 'suspect (count "suspect")
          'safe (count "safe") 'unanalyzed (count "unanalyzed") 'errors (count "error")))

;; ---------------------------------------------------------------------------
;; CLI

(define (main argv)
  (define file #f)
  (define json? #f)
  (define patterns '())
  (define fail-on "quadratic")
  (define flags "")
  (define mode "search")
  (define max-steps (opts-max-steps DEFAULT-OPTS))
  (define max-positions (opts-max-positions DEFAULT-OPTS))
  (define max-attack (opts-max-attack DEFAULT-OPTS))
  (define (num s what)
    (define v (string->number s))
    (unless (and v (exact-positive-integer? v))
      (eprintf "~a needs a positive integer, got ~a\n" what s)
      (exit 2))
    v)
  (define parsed
    (let/ec esc
      (parameterize ([exit-handler (lambda (c) (esc (cons 'exit c)))])
        (with-handlers ([exn:fail? (lambda (e) (eprintf "~a\n" (exn-message e)) (esc (cons 'exit 2)))])
        (command-line
         #:program "RegexBacktrackProver"
         #:argv argv
         #:once-each
         [("-f" "--file") p "File with one pattern per line, default stdin" (set! file p)]
         [("--json") "Emit JSON lines instead of text" (set! json? #t)]
         [("--fail-on") l "exponential, cubic, quadratic, suspect or none (default quadratic)" (set! fail-on l)]
         [("--flags") f "Flags applied to every pattern, letters i and s matter" (set! flags f)]
         [("--mode") m "search (default) or anchored" (set! mode m)]
         [("--max-steps") n "Step budget per match (default 2000000)" (set! max-steps (num n "--max-steps"))]
         [("--max-positions") n "Position budget after expanding repeats (default 300)" (set! max-positions (num n "--max-positions"))]
         [("--max-attack") n "Longest attack string in characters (default 1024)" (set! max-attack (num n "--max-attack"))]
         #:multi
         [("-p" "--pattern") p "Analyze this pattern, repeatable" (set! patterns (cons p patterns))]
         #:args ()
         'ok)))))
  (when (pair? parsed)
    (exit (if (zero? (cdr parsed)) 0 2)))
  (unless (member fail-on FAIL-LEVELS)
    (eprintf "--fail-on must be one of ~a\n" (string-join FAIL-LEVELS ", "))
    (exit 2))
  (unless (member mode '("search" "anchored"))
    (eprintf "--mode must be search or anchored\n")
    (exit 2))
  (define lines
    (cond
      [(pair? patterns) (map (lambda (p) (jsexpr->string (hasheq 'pattern p))) (reverse patterns))]
      [file
       (with-handlers ([exn:fail:filesystem?
                        (lambda (e) (eprintf "cannot read ~a: ~a\n" file (exn-message e)) (exit 2))])
         (call-with-input-file file port->lines))]
      [else (port->lines (current-input-port))]))
  (define o (opts max-steps max-positions (string=? mode "search") max-attack
                  (opts-ida-budget DEFAULT-OPTS) (opts-max-witnesses DEFAULT-OPTS)))
  (define results (analyze-lines lines flags o))
  (for ([r (in-list results)])
    (cond [json? (write-json r) (newline)]
          [else (displayln (render-text r))]))
  (define s (summary results))
  (if json?
      (begin (write-json s) (newline))
      (printf "\n~a patterns: ~a vulnerable, ~a suspect, ~a safe, ~a unanalyzed, ~a errors\n"
              (hash-ref s 'patterns) (hash-ref s 'vulnerable) (hash-ref s 'suspect)
              (hash-ref s 'safe) (hash-ref s 'unanalyzed) (hash-ref s 'errors)))
  (exit (exit-code-for results fail-on)))

(module+ main
  (main (current-command-line-arguments)))

(module+ test
  (require rackunit)
  (define (verdict p [flags ""])
    (define r (analyze-pattern p flags))
    (list (hash-ref r 'status) (hash-ref r 'verdict #f) (hash-ref r 'degree #f)))
  (check-equal? (take (verdict "^(a+)+$") 2) '("vulnerable" "exponential"))
  (check-equal? (take (verdict "(a+)+$") 2) '("vulnerable" "exponential"))
  (check-equal? (take (verdict "^(a|aa)+$") 2) '("vulnerable" "exponential"))
  (check-equal? (take (verdict "^(\\w+\\s?)*$") 2) '("vulnerable" "exponential"))
  (check-equal? (take (verdict "^([a-z]+\\.?)+@x$") 2) '("vulnerable" "exponential"))
  (check-equal? (verdict "^(a|b)*c$") '("safe" #f #f))
  (check-equal? (verdict "^[a-z0-9_]{3,16}$") '("safe" #f #f))
  (check-equal? (verdict "^\\d{4}-\\d{2}-\\d{2}$") '("safe" #f #f))
  (check-equal? (verdict "\\s+$") '("vulnerable" "polynomial" 2))
  (check-equal? (verdict "^a*a*$") '("vulnerable" "polynomial" 2))
  (check-true (>= (third (verdict "^a*a*a*$")) 3))
  (check-equal? (hash-ref (analyze-pattern "(" "") 'status) "error")
  (check-equal? (hash-ref (analyze-pattern "*a" "") 'status) "error")
  (check-equal? (hash-ref (analyze-pattern "[z-a]" "") 'status) "error")
  (check-equal? (hash-ref (analyze-pattern "(a)\\2" "") 'status) "error")
  (check-equal? (hash-ref (analyze-pattern "^(?>a+)+$" "") 'status) "suspect")
  (check-equal? (hash-ref (analyze-pattern "^(a++)+$" "") 'status) "suspect")
  (check-equal? (hash-ref (analyze-pattern "^(a+)+$" "") 'status) "vulnerable")
  (check-equal? (hash-ref (analyze-pattern "(a+)+" "") 'status) "suspect")
  (check-equal? (hash-ref (analyze-pattern "^(a?){25}a{25}$" "") 'verdict) "bounded")
  (check-equal? (hash-ref (analyze-pattern "^[a-z]+$" "") 'status) "safe")
  (check-equal? (hash-ref (analyze-pattern "^(.*?,){11}P" "") 'verdict) "polynomial")
  (check-equal? (hash-ref (analyze-pattern "^(x{1,300}){1,300}$" "") 'status) "unanalyzed")
  (let-values ([(ok? steps ex?)
                (let-values ([(ast ng notes) (parse-regex "^(a+)+$")])
                  (run-match ast ng "aaaaaaaaaaaaaaaaaaaaaaaaaaaa!" #t 1000))])
    (check-false ok?)
    (check-true ex?))
  (let-values ([(ast ng notes) (parse-regex "^(\\d+)-\\1$")])
    (let-values ([(ok? steps ex?) (run-match ast ng "42-42" #t 1000)])
      (check-true ok?))
    (let-values ([(ok? steps ex?) (run-match ast ng "42-43" #t 1000)])
      (check-false ok?)))
  (let-values ([(ast ng notes) (parse-regex "^HELLO$" #:icase? #t)])
    (let-values ([(ok? steps ex?) (run-match ast ng "hello" #t 1000)])
      (check-true ok?)))
  (check-equal? (parse-input-line "/a+b/i" 7) '(7 "a+b" "i"))
  (check-equal? (parse-input-line "# note" 1) #f)
  (check-equal? (parse-input-line "{\"id\":\"x\",\"pattern\":\"a\"}" 1) '("x" "a" ""))
  (check-equal? (exit-code-for (list (analyze-pattern "^(a+)+$" "")) "exponential") 1)
  (check-equal? (exit-code-for (list (analyze-pattern "^a$" "")) "quadratic") 0)
  (check-equal? (exit-code-for (list (analyze-pattern "(" "")) "quadratic") 3))
