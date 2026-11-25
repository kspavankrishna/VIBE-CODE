#lang racket/base

;; Network Policy Reachability Auditor
;; Replays Kubernetes NetworkPolicy semantics over a cluster snapshot or rendered
;; manifests, answers "can A reach B on this port" with the exact rule that decides it
;; and gates CI on reachability expectations.

(require json
         racket/cmdline
         racket/list
         racket/match
         racket/port
         racket/string)

(provide (struct-out wl)
         (struct-out ext)
         (struct-out policy)
         (struct-out finding)
         (struct-out side)
         parse-ip
         parse-cidr
         parse-selector
         selector-matches?
         parse-port-arg
         build-model
         objects-from-jsexprs
         pair-eval
         resolve-endpoint
         run-audit
         run-check
         main)

;; ---------------------------------------------------------------------------
;; Constants

(define SYSTEM-NAMESPACES '("kube-system" "kube-public" "kube-node-lease"))
(define NS-NAME-LABEL "kubernetes.io/metadata.name")
(define DEFAULT-DNS-SPEC "kube-system:k8s-app=kube-dns")
(define VALID-PROTOCOLS '("TCP" "UDP" "SCTP"))
(define TEMPLATE-KINDS '("Deployment" "StatefulSet" "DaemonSet" "ReplicaSet" "ReplicationController" "Job" "CronJob"))
(define UNMODELED-POLICY-KINDS
  '("AdminNetworkPolicy" "BaselineAdminNetworkPolicy" "ClusterNetworkPolicy"
    "CiliumNetworkPolicy" "CiliumClusterwideNetworkPolicy"
    "GlobalNetworkPolicy" "GlobalNetworkSet" "AuthorizationPolicy"))

;; ---------------------------------------------------------------------------
;; Findings

(struct finding (severity code subject message) #:transparent)

(define current-findings (make-parameter #f))

(define (add-finding! sev code subject fmt . args)
  (define box (current-findings))
  (when box
    (set-box! box (cons (finding sev code subject (apply format fmt args)) (unbox box)))))

(define (severity-rank s)
  (case s [(error) 0] [(warn) 1] [(info) 2] [else 3]))

(define (fail-exn fmt . args)
  (raise (exn:fail:user (apply format fmt args) (current-continuation-marks))))

;; ---------------------------------------------------------------------------
;; JSON helpers

(define (jref h . keys)
  (let loop ([v h] [ks keys])
    (cond [(null? ks) (if (eq? v 'null) #f v)]
          [(hash? v) (loop (hash-ref v (car ks) #f) (cdr ks))]
          [else #f])))

(define (jlist v) (if (list? v) v '()))

(define (labels-of h)
  (if (hash? h)
      (for/hash ([(k v) (in-hash h)] #:when (string? v))
        (values (if (symbol? k) (symbol->string k) k) v))
      (hash)))

;; ---------------------------------------------------------------------------
;; IP addresses and CIDRs. An address is (cons family integer).

(define (family-bits fam) (if (= fam 4) 32 128))

(define (parse-ipv4 s)
  (define parts (string-split s "." #:trim? #f))
  (and (= 4 (length parts))
       (for/and ([p (in-list parts)])
         (and (regexp-match? #px"^[0-9]{1,3}$" p)
              (not (and (> (string-length p) 1) (char=? (string-ref p 0) #\0)))
              (<= (string->number p) 255)))
       (for/fold ([acc 0]) ([p (in-list parts)])
         (+ (* acc 256) (string->number p)))))

(define (ipv6-groups str allow-v4-tail?)
  ;; returns a list of 16 bit integers or #f
  (define raw (if (string=? str "") '() (string-split str ":" #:trim? #f)))
  (let loop ([gs raw] [acc '()])
    (cond
      [(null? gs) (reverse acc)]
      [(and allow-v4-tail? (null? (cdr gs)) (regexp-match? #rx"[.]" (car gs)))
       (define v4 (parse-ipv4 (car gs)))
       (and v4 (reverse (list* (bitwise-and v4 #xffff) (arithmetic-shift v4 -16) acc)))]
      [(regexp-match? #px"^[0-9a-fA-F]{1,4}$" (car gs))
       (loop (cdr gs) (cons (string->number (car gs) 16) acc))]
      [else #f])))

(define (parse-ipv6 s0)
  (define s (car (string-split s0 "%" #:trim? #f)))
  (define halves (regexp-split #rx"::" s))
  (define groups
    (case (length halves)
      [(1) (let ([g (ipv6-groups (car halves) #t)]) (and g (= (length g) 8) g))]
      [(2) (let ([l (ipv6-groups (car halves) #f)]
                 [r (ipv6-groups (cadr halves) #t)])
             (and l r (<= (+ (length l) (length r)) 7)
                  (append l (make-list (- 8 (length l) (length r)) 0) r)))]
      [else #f]))
  (and groups
       (for/fold ([acc 0]) ([g (in-list groups)])
         (+ (* acc 65536) g))))

(define (parse-ip s)
  (cond [(not (string? s)) #f]
        [(regexp-match? #rx":" s) (let ([v (parse-ipv6 s)]) (and v (cons 6 v)))]
        [else (let ([v (parse-ipv4 s)]) (and v (cons 4 v)))]))

(define (ip->string ip)
  (if (= (car ip) 4)
      (string-join (for/list ([sh (in-list '(24 16 8 0))])
                     (number->string (bitwise-and (arithmetic-shift (cdr ip) (- sh)) 255))) ".")
      (string-join (for/list ([sh (in-range 112 -1 -16)])
                     (number->string (bitwise-and (arithmetic-shift (cdr ip) (- sh)) #xffff) 16)) ":")))

(struct cidr (fam net len text canonical?) #:transparent)

(define (parse-cidr s)
  (match (and (string? s) (string-split s "/" #:trim? #f))
    [(list a l)
     (define ip (parse-ip a))
     (define n (and (regexp-match? #px"^[0-9]{1,3}$" l) (string->number l)))
     (and ip n (<= n (family-bits (car ip)))
          (let* ([shift (- (family-bits (car ip)) n)]
                 [net (arithmetic-shift (arithmetic-shift (cdr ip) (- shift)) shift)])
            (cidr (car ip) net n s (= net (cdr ip)))))]
    [_ #f]))

(define (cidr-prefix c v len)
  (arithmetic-shift v (- (- (family-bits (cidr-fam c)) len))))

(define (cidr-contains? c ip)
  (and (= (cidr-fam c) (car ip))
       (= (cidr-prefix c (cdr ip) (cidr-len c))
          (cidr-prefix c (cidr-net c) (cidr-len c)))))

(define (cidr-overlaps? a b)
  (and (= (cidr-fam a) (cidr-fam b))
       (let ([m (min (cidr-len a) (cidr-len b))])
         (= (cidr-prefix a (cidr-net a) m) (cidr-prefix a (cidr-net b) m)))))

(define (cidr-within? inner outer)
  (and (= (cidr-fam inner) (cidr-fam outer))
       (>= (cidr-len inner) (cidr-len outer))
       (cidr-contains? outer (cons (cidr-fam inner) (cidr-net inner)))))

(struct ipblock (main excepts) #:transparent)

(define (block-contains? b ip)
  (and (cidr-contains? (ipblock-main b) ip)
       (not (for/or ([e (in-list (ipblock-excepts b))]) (cidr-contains? e ip)))))

(define (block-overlaps-cidr? b pc)
  (and (cidr-overlaps? (ipblock-main b) pc)
       (not (for/or ([e (in-list (ipblock-excepts b))]) (cidr-within? pc e)))))

;; ---------------------------------------------------------------------------
;; Label selectors. Expressions are (list op key values). 'Never matches nothing
;; and stands in for a selector the API server would have rejected.

(struct selector (labels exprs) #:transparent)

(define (selector-empty? s)
  (and (null? (selector-labels s)) (null? (selector-exprs s))))

(define NEVER-SELECTOR (selector '() (list (list 'Never "" '()))))

(define (parse-selector h where)
  (cond
    [(not (hash? h)) (selector '() '())]
    [else
     (define ml
       (for/list ([(k v) (in-hash (or (jref h 'matchLabels) (hash)))])
         (cons (symbol->string k) v)))
     (define bad-ml (filter (lambda (kv) (not (string? (cdr kv)))) ml))
     (define exprs
       (for/list ([e (in-list (jlist (jref h 'matchExpressions)))])
         (define op (jref e 'operator))
         (define key (jref e 'key))
         (define vals (jlist (jref e 'values)))
         (cond
           [(not (string? key))
            (add-finding! 'error "invalid-selector" where "matchExpressions entry without a key, selector matches nothing")
            (list 'Never "" '())]
           [(member op '("In" "NotIn"))
            (cond [(null? vals)
                   (add-finding! 'error "invalid-selector" where
                                 "operator ~a on key ~a has no values, the API server rejects this and the selector matches nothing here" op key)
                   (list 'Never key '())]
                  [else (list (string->symbol op) key vals)])]
           [(member op '("Exists" "DoesNotExist"))
            (unless (null? vals)
              (add-finding! 'error "invalid-selector" where
                            "operator ~a on key ~a must not carry values, the API server rejects this" op key))
            (if (null? vals) (list (string->symbol op) key '()) (list 'Never key '()))]
           [else
            (add-finding! 'error "invalid-selector" where
                          "unknown operator ~s on key ~a, selector matches nothing" op key)
            (list 'Never key '())])))
     (cond [(pair? bad-ml)
            (add-finding! 'error "invalid-selector" where "matchLabels value for ~a is not a string" (car (car bad-ml)))
            NEVER-SELECTOR]
           [else (selector ml exprs)])]))

(define (selector-matches? s labels)
  (and (for/and ([kv (in-list (selector-labels s))])
         (equal? (hash-ref labels (car kv) #f) (cdr kv)))
       (for/and ([e (in-list (selector-exprs s))])
         (match e
           [(list 'In k vs) (let ([v (hash-ref labels k #f)]) (and v (member v vs) #t))]
           [(list 'NotIn k vs) (let ([v (hash-ref labels k #f)]) (not (and v (member v vs))))]
           [(list 'Exists k _) (hash-has-key? labels k)]
           [(list 'DoesNotExist k _) (not (hash-has-key? labels k))]
           [_ #f]))))

(define (selector->string s)
  (define parts
    (append
     (for/list ([kv (in-list (selector-labels s))]) (format "~a=~a" (car kv) (cdr kv)))
     (for/list ([e (in-list (selector-exprs s))])
       (match e
         [(list 'In k vs) (format "~a in (~a)" k (string-join vs ","))]
         [(list 'NotIn k vs) (format "~a notin (~a)" k (string-join vs ","))]
         [(list 'Exists k _) k]
         [(list 'DoesNotExist k _) (format "!~a" k)]
         [_ "<invalid>"]))))
  (if (null? parts) "{}" (string-join parts ",")))

;; ---------------------------------------------------------------------------
;; Ports

(struct pspec (proto port end) #:transparent)

(define (valid-port-name? s)
  (and (regexp-match? #px"^[a-z0-9]([a-z0-9-]{0,13}[a-z0-9])?$" s)
       (regexp-match? #px"[a-z]" s)
       (not (regexp-match? #rx"--" s))))

(define (parse-pspec h where)
  (define proto-raw (or (jref h 'protocol) "TCP"))
  (define proto (and (string? proto-raw) (string-upcase proto-raw)))
  (define port (jref h 'port))
  (define end (jref h 'endPort))
  (cond
    [(not (and proto (member proto VALID-PROTOCOLS)))
     (add-finding! 'error "invalid-port" where "protocol ~s is not TCP, UDP or SCTP, entry matches nothing" proto-raw)
     (pspec "NONE" 'never #f)]
    [(and end (not (exact-integer? port)))
     (add-finding! 'error "invalid-port" where "endPort is only valid with a numeric port, entry matches nothing")
     (pspec proto 'never #f)]
    [(and end (or (not (exact-integer? end)) (< end port) (> end 65535)))
     (add-finding! 'error "invalid-port" where "endPort ~a must be a number between port ~a and 65535, entry matches nothing" end port)
     (pspec proto 'never #f)]
    [(not port) (pspec proto #f #f)]
    [(exact-integer? port)
     (if (<= 1 port 65535)
         (pspec proto port end)
         (begin (add-finding! 'error "invalid-port" where "port ~a is outside 1-65535, entry matches nothing" port)
                (pspec proto 'never #f)))]
    [(string? port)
     (cond [(regexp-match? #px"^[0-9]+$" port)
            (add-finding! 'error "invalid-port" where
                          "port ~s is a quoted number, the API treats it as a port name and rejects it; write it without quotes" port)
            (pspec proto 'never #f)]
           [else
            (unless (valid-port-name? port)
              (add-finding! 'warn "invalid-port" where "port name ~s is not a valid IANA service name" port))
            (pspec proto port #f)])]
    [else
     (add-finding! 'error "invalid-port" where "port ~s is neither a number nor a name" port)
     (pspec proto 'never #f)]))

(define (pspec->string ps)
  (define p (pspec-port ps))
  (cond [(eq? p 'never) "<invalid>"]
        [(not p) (format "any/~a" (pspec-proto ps))]
        [(pspec-end ps) (format "~a-~a/~a" p (pspec-end ps) (pspec-proto ps))]
        [else (format "~a/~a" p (pspec-proto ps))]))

(define (parse-port-arg s)
  (match (regexp-match #px"^([0-9]{1,5})(?:/([A-Za-z]+))?$" s)
    [(list _ n proto)
     (define port (string->number n))
     (define pr (string-upcase (or proto "TCP")))
     (unless (and (<= 1 port 65535) (member pr VALID-PROTOCOLS))
       (fail-exn "bad port ~s: expected 1-65535 with optional /TCP, /UDP or /SCTP" s))
     (cons port pr)]
    [_ (fail-exn "bad port ~s: expected something like 5432/TCP or 53/UDP" s)]))

;; ---------------------------------------------------------------------------
;; Model structures

;; A workload class: every pod in it has the same namespace, labels, named ports
;; and host networking, so policy treats them identically (ipBlock aside).
(struct wl (id ns name labels ports host? ips pods source))

;; An address outside the pod network, named on the command line with ip:ADDR.
(struct ext (addr text))

(struct peer (idx pod-sel ns-sel block) #:transparent)
(struct rule (dir idx peers ports) #:transparent)
(struct policy (ns name selector types ingress egress) #:transparent)

(struct model (namespaces classes policies pod-cidrs))

(define (testable-pair? s d)
  ;; a single pod talking to itself is loopback, never policy controlled
  (not (and (eq? s d) (wl? s) (<= (length (wl-pods s)) 1) (eq? (wl-source s) 'pod))))

(define (endpoint-name ep)
  (if (ext? ep) (format "ip:~a" (ext-text ep)) (wl-id ep)))

(define (policy-id p) (format "~a/~a" (policy-ns p) (policy-name p)))

;; ---------------------------------------------------------------------------
;; Reading Kubernetes objects

(define (objects-from-jsexprs vs)
  (define (walk v inferred-kind)
    (cond
      [(list? v) (append-map (lambda (x) (walk x inferred-kind)) v)]
      [(hash? v)
       (define items (jref v 'items))
       (define kind (jref v 'kind))
       (cond
         [(list? items)
          (define item-kind
            (and (string? kind) (regexp-match? #rx"List$" kind) (not (string=? kind "List"))
                 (substring kind 0 (- (string-length kind) 4))))
          (walk items item-kind)]
         [(and (not kind) inferred-kind) (list (hash-set v 'kind inferred-kind))]
         [else (list v)])]
      [else '()]))
  (walk vs #f))

(define (read-json-documents in source)
  (let loop ([acc '()])
    (define v
      (with-handlers ([exn:fail:read?
                       (lambda (e) (fail-exn "~a: not valid JSON (~a). Convert YAML first, for example with yq -o=json"
                                             source (exn-message e)))])
        (read-json in)))
    (if (eof-object? v) (reverse acc) (loop (cons v acc)))))

(define (container-ports spec)
  (define cs (append (jlist (jref spec 'containers)) (jlist (jref spec 'initContainers))))
  (remove-duplicates
   (sort
    (for*/list ([c (in-list cs)]
                [p (in-list (jlist (jref c 'ports)))]
                #:when (exact-integer? (jref p 'containerPort)))
      (list (or (jref p 'name) "")
            (jref p 'containerPort)
            (string-upcase (or (jref p 'protocol) "TCP"))))
    (lambda (a b) (string<? (format "~a" a) (format "~a" b))))))

(define (pod-display-name obj labels)
  (define name (or (jref obj 'metadata 'name) "unnamed"))
  (define owner (let ([os (jlist (jref obj 'metadata 'ownerReferences))]) (and (pair? os) (car os))))
  (define owner-name (and owner (jref owner 'name)))
  (define owner-kind (and owner (jref owner 'kind)))
  (cond
    [(and owner-name (equal? owner-kind "ReplicaSet"))
     (define h (hash-ref labels "pod-template-hash" #f))
     (if (and h (string-suffix? owner-name (string-append "-" h)))
         (substring owner-name 0 (- (string-length owner-name) (string-length h) 1))
         owner-name)]
    [owner-name owner-name]
    [else name]))

(define (template-of obj kind)
  (if (equal? kind "CronJob")
      (jref obj 'spec 'jobTemplate 'spec 'template)
      (jref obj 'spec 'template)))

(define (parse-peer h idx where pol-ns)
  (define pw (format "~a[~a]" where idx))
  (define ps (jref h 'podSelector))
  (define nss (jref h 'namespaceSelector))
  (define ib (jref h 'ipBlock))
  (cond
    [(and ib (or ps nss))
     (add-finding! 'error "invalid-peer" pw "ipBlock cannot be combined with selectors in one peer, peer matches nothing")
     (peer idx NEVER-SELECTOR #f #f)]
    [ib
     (define main (parse-cidr (jref ib 'cidr)))
     (cond
       [(not main)
        (add-finding! 'error "invalid-cidr" pw "ipBlock.cidr ~s is not a valid CIDR, peer matches nothing" (jref ib 'cidr))
        (peer idx NEVER-SELECTOR #f #f)]
       [else
        (unless (cidr-canonical? main)
          (add-finding! 'warn "noncanonical-cidr" pw
                        "ipBlock.cidr ~a has host bits set, it is treated as ~a/~a" (cidr-text main)
                        (ip->string (cons (cidr-fam main) (cidr-net main))) (cidr-len main)))
        (define excepts
          (for*/list ([e (in-list (jlist (jref ib 'except)))]
                      [c (in-value (parse-cidr e))]
                      #:when (or c (begin (add-finding! 'error "invalid-cidr" pw "except entry ~s is not a valid CIDR, ignored" e) #f)))
            (unless (cidr-within? c main)
              (add-finding! 'warn "except-outside-cidr" pw
                            "except ~a is not inside ~a, the API server rejects this" (cidr-text c) (cidr-text main)))
            c))
        (peer idx #f #f (ipblock main excepts))])]
    [(or ps nss)
     (peer idx
           (and ps (parse-selector ps (string-append pw ".podSelector")))
           (and nss (parse-selector nss (string-append pw ".namespaceSelector")))
           #f)]
    [else
     (add-finding! 'error "invalid-peer" pw "empty peer object, the API server rejects this, peer matches nothing")
     (peer idx NEVER-SELECTOR #f #f)]))

(define (parse-rules lst dir pid pol-ns)
  (define peer-key (if (eq? dir 'Ingress) 'from 'to))
  (define dir-key (if (eq? dir 'Ingress) "ingress" "egress"))
  (for/list ([r (in-list lst)] [i (in-naturals)])
    (define where (format "~a spec.~a[~a]" pid dir-key i))
    (define peers-raw (jlist (jref r peer-key)))
    (define ports-raw (jlist (jref r 'ports)))
    (rule dir i
          (if (null? peers-raw)
              'all
              (for/list ([p (in-list peers-raw)] [j (in-naturals)])
                (parse-peer p j (format "~a.~a" where peer-key) pol-ns)))
          (if (null? ports-raw)
              'all
              (for/list ([p (in-list ports-raw)] [j (in-naturals)])
                (parse-pspec p (format "~a.ports[~a]" where j)))))))

(define (parse-policy obj default-ns)
  (define ns (or (jref obj 'metadata 'namespace) default-ns))
  (define name (or (jref obj 'metadata 'name) "unnamed"))
  (define pid (format "~a/~a" ns name))
  (define spec (or (jref obj 'spec) (hash)))
  (define ing (jlist (jref spec 'ingress)))
  (define egr (jlist (jref spec 'egress)))
  (define declared (jref spec 'policyTypes))
  (define types
    (cond
      [(list? declared)
       (for/list ([t (in-list declared)]
                  #:when (or (member t '("Ingress" "Egress"))
                             (begin (add-finding! 'error "invalid-policy-types" pid "unknown policyType ~s ignored" t) #f)))
         (string->symbol t))]
      [else (append '(Ingress) (if (pair? egr) '(Egress) '()))]))
  (when (and (list? declared) (pair? egr) (not (memq 'Egress types)))
    (add-finding! 'warn "ignored-rules" pid
                  "has ~a egress rule(s) but policyTypes does not list Egress, so those rules are never applied" (length egr)))
  (when (and (list? declared) (pair? ing) (not (memq 'Ingress types)))
    (add-finding! 'warn "ignored-rules" pid
                  "has ~a ingress rule(s) but policyTypes does not list Ingress, so those rules are never applied" (length ing)))
  (unless (jref spec 'podSelector)
    (add-finding! 'warn "missing-pod-selector" pid "spec.podSelector is missing, treated as {} which selects every pod in ~a" ns))
  (policy ns name
          (parse-selector (jref spec 'podSelector) (string-append pid " spec.podSelector"))
          types
          (parse-rules ing 'Ingress pid ns)
          (parse-rules egr 'Egress pid ns)))

(define (subset-labels? small big)
  (for/and ([(k v) (in-hash small)]) (equal? (hash-ref big k #f) v)))

(define (build-model objs #:default-namespace [default-ns "default"] #:pod-cidrs [pod-cidrs '()])
  (define namespaces (make-hash))
  (define pods '())
  (define templates '())
  (define policies '())
  (define unmodeled (make-hash))
  (for ([o (in-list objs)])
    (define kind (jref o 'kind))
    (cond
      [(equal? kind "Namespace")
       (define name (jref o 'metadata 'name))
       (when (string? name)
         (hash-set! namespaces name (hash-set (labels-of (jref o 'metadata 'labels)) NS-NAME-LABEL name)))]
      [(equal? kind "Pod")
       (define phase (jref o 'status 'phase))
       (unless (member phase '("Succeeded" "Failed"))
         (set! pods (cons o pods)))]
      [(member kind TEMPLATE-KINDS)
       (define owned-by-controller?
         (for/or ([r (in-list (jlist (jref o 'metadata 'ownerReferences)))])
           (member (jref r 'kind) '("Deployment" "CronJob"))))
       (unless owned-by-controller? (set! templates (cons o templates)))]
      [(equal? kind "NetworkPolicy")
       (set! policies (cons (parse-policy o default-ns) policies))]
      [(member kind UNMODELED-POLICY-KINDS)
       (hash-update! unmodeled kind add1 0)]
      [else (void)]))
  (for ([(k n) (in-hash unmodeled)])
    (add-finding! 'warn "unmodeled-policy" k
                  "~a object(s) of kind ~a found; they can allow or deny traffic before NetworkPolicy runs and are not modelled here" n k))
  ;; group live pods into classes
  (define groups (make-hash))
  (define group-order '())
  (for ([o (in-list (reverse pods))])
    (define ns (or (jref o 'metadata 'namespace) default-ns))
    (define labels (labels-of (jref o 'metadata 'labels)))
    (define spec (or (jref o 'spec) (hash)))
    (define ports (container-ports spec))
    (define host? (eq? #t (jref spec 'hostNetwork)))
    (define key (list ns labels ports host?))
    (define ips
      (remove-duplicates
       (filter values
               (append (list (parse-ip (jref o 'status 'podIP)))
                       (for/list ([p (in-list (jlist (jref o 'status 'podIPs)))]) (parse-ip (jref p 'ip)))))))
    (define pname (or (jref o 'metadata 'name) "unnamed"))
    (unless (hash-has-key? groups key) (set! group-order (cons key group-order)))
    (hash-update! groups key
                  (lambda (g) (list (car g) (append (cadr g) ips) (append (caddr g) (list pname))))
                  (list (pod-display-name o labels) '() '())))
  (define pod-classes
    (for/list ([key (in-list (reverse group-order))])
      (match-define (list ns labels ports host?) key)
      (match-define (list name ips pnames) (hash-ref groups key))
      (list ns name labels ports host? ips pnames 'pod)))
  (define template-classes
    (for*/list ([o (in-list (reverse templates))]
                [kind (in-value (jref o 'kind))]
                [tpl (in-value (template-of o kind))]
                #:when (hash? tpl))
      (define ns (or (jref o 'metadata 'namespace) default-ns))
      (define labels (labels-of (jref tpl 'metadata 'labels)))
      (define spec (or (jref tpl 'spec) (hash)))
      (list ns (or (jref o 'metadata 'name) "unnamed") labels (container-ports spec)
            (eq? #t (jref spec 'hostNetwork)) '() '() (string->symbol (string-downcase kind)))))
  (define kept-templates
    (filter (lambda (t)
              (not (for/or ([p (in-list pod-classes)])
                     (and (equal? (car p) (car t)) (subset-labels? (caddr t) (caddr p))))))
            template-classes))
  (define seen-ids (make-hash))
  (define classes
    (for/list ([c (in-list (append pod-classes kept-templates))])
      (match-define (list ns name labels ports host? ips pnames source) c)
      (define base (format "~a/~a" ns name))
      (define n (add1 (hash-ref seen-ids base 0)))
      (hash-set! seen-ids base n)
      (wl (if (= n 1) base (format "~a#~a" base n)) ns name labels ports host? ips pnames source)))
  ;; synthesize namespaces that are referenced but not supplied
  (define referenced
    (remove-duplicates (append (map wl-ns classes) (map policy-ns policies))))
  (define missing (filter (lambda (n) (not (hash-has-key? namespaces n))) referenced))
  (for ([n (in-list missing)])
    (hash-set! namespaces n (hash NS-NAME-LABEL n)))
  (when (pair? missing)
    (add-finding! 'warn "missing-namespace" (string-join (sort missing string<?) ",")
                  "no Namespace object supplied, namespaceSelectors see only the ~a label for these" NS-NAME-LABEL))
  (model namespaces classes (reverse policies) pod-cidrs))

;; ---------------------------------------------------------------------------
;; Evaluation

;; Matching results are 'yes, 'maybe (depends on how the CNI treats ipBlock
;; against pod addresses) or 'no.

(define (best a b)
  (cond [(or (eq? a 'yes) (eq? b 'yes)) 'yes]
        [(or (eq? a 'maybe) (eq? b 'maybe)) 'maybe]
        [else 'no]))

(define (ns-labels m ns) (hash-ref (model-namespaces m) ns (lambda () (hash NS-NAME-LABEL ns))))

(define (peer-match m pol p ep)
  (cond
    [(peer-block p)
     (define b (peer-block p))
     (cond
       [(ext? ep) (if (block-contains? b (ext-addr ep)) 'yes 'no)]
       [(pair? (wl-ips ep))
        (if (for/or ([ip (in-list (wl-ips ep))]) (block-contains? b ip)) 'maybe 'no)]
       [(for/or ([pc (in-list (model-pod-cidrs m))]) (block-overlaps-cidr? b pc)) 'maybe]
       [else 'no])]
    [(ext? ep) 'no]
    [else
     (define ns-ok
       (if (peer-ns-sel p)
           (selector-matches? (peer-ns-sel p) (ns-labels m (wl-ns ep)))
           (equal? (wl-ns ep) (policy-ns pol))))
     (define pod-ok (or (not (peer-pod-sel p)) (selector-matches? (peer-pod-sel p) (wl-labels ep))))
     (if (and ns-ok pod-ok) 'yes 'no)]))

(define (port-match? ps target port proto)
  (define p (pspec-port ps))
  (cond
    [(not (string=? (pspec-proto ps) proto)) #f]
    [(eq? p 'never) #f]
    [(not p) #t]
    [(exact-integer? p) (if (pspec-end ps) (<= p port (pspec-end ps)) (= p port))]
    [(string? p)
     (and (wl? target)
          (for/or ([cp (in-list (wl-ports target))])
            (and (equal? (car cp) p) (= (cadr cp) port) (string=? (caddr cp) proto))))]
    [else #f]))

(define (rule-where pol r)
  (format "~a spec.~a[~a]" (policy-id pol) (if (eq? (rule-dir r) 'Ingress) "ingress" "egress") (rule-idx r)))

(define (rule-eval m pol r peer-ep target port proto)
  (define ports-ok
    (or (eq? (rule-ports r) 'all)
        (for/or ([ps (in-list (rule-ports r))]) (port-match? ps target port proto))))
  (cond
    [(not ports-ok) (values 'no #f)]
    [(eq? (rule-peers r) 'all) (values 'yes (format "~a (any peer)" (rule-where pol r)))]
    [else
     (define key (if (eq? (rule-dir r) 'Ingress) "from" "to"))
     (let loop ([ps (rule-peers r)] [res 'no] [w #f])
       (cond
         [(null? ps) (values res w)]
         [else
          (define x (peer-match m pol (car ps) peer-ep))
          (define here (format "~a.~a[~a]" (rule-where pol r) key (peer-idx (car ps))))
          (case x
            [(yes) (values 'yes here)]
            [(maybe) (loop (cdr ps) (best res 'maybe) (or w (string-append here " (ipBlock over pod IPs)")))]
            [else (loop (cdr ps) res w)])]))]))

(struct side (dir verdict policies witnesses) #:transparent)

(define (make-selecting-cache) (make-hasheq))

(define current-selecting-cache (make-parameter #f))

(define (selecting-policies m w dir)
  (define cache (current-selecting-cache))
  (define (compute)
    (for/list ([p (in-list (model-policies m))]
               #:when (and (equal? (policy-ns p) (wl-ns w))
                           (memq dir (policy-types p))
                           (selector-matches? (policy-selector p) (wl-labels w))))
      p))
  (if cache
      (hash-ref! (hash-ref! cache w make-hasheq) dir compute)
      (compute)))

(define (side-eval m dir subject peer-ep target port proto)
  (cond
    [(ext? subject) (side dir 'open '() (list "outside the pod network, no NetworkPolicy applies"))]
    [else
     (define pols (selecting-policies m subject dir))
     (cond
       [(null? pols) (side dir 'open '() (list (format "no policy isolates ~a of ~a" (string-downcase (symbol->string dir)) (wl-id subject))))]
       [(wl-host? subject)
        (side dir 'maybe pols (list (format "~a uses hostNetwork, NetworkPolicy enforcement for it is CNI defined" (wl-id subject))))]
       [else
        (define-values (verdict ws)
          (for*/fold ([v 'no] [ws '()])
                     ([p (in-list pols)]
                      [r (in-list (if (eq? dir 'Ingress) (policy-ingress p) (policy-egress p)))])
            (define-values (x w) (rule-eval m p r peer-ep target port proto))
            (if (eq? x 'no) (values v ws) (values (best v x) (cons (cons x w) ws)))))
        (define kept
          (for/list ([x (in-list (reverse ws))] #:when (or (eq? verdict 'maybe) (eq? (car x) 'yes))) (cdr x)))
        (side dir verdict pols
              (if (eq? verdict 'no)
                  (list (format "isolated by ~a, no rule admits this traffic"
                                (string-join (map policy-id pols) ", ")))
                  kept))])]))

(define (side-allows s) (case (side-verdict s) [(open yes) 'allow] [(maybe) 'depends] [else 'deny]))

(define (pair-eval m src dst port proto)
  (define eg (side-eval m 'Egress src dst dst port proto))
  (define in (side-eval m 'Ingress dst src dst port proto))
  (define a (side-allows eg))
  (define b (side-allows in))
  (values (cond [(or (eq? a 'deny) (eq? b 'deny)) 'deny]
                [(or (eq? a 'depends) (eq? b 'depends)) 'depends]
                [else 'allow])
          eg in))

;; ---------------------------------------------------------------------------
;; Endpoint specs on the command line and in expectation files
;;   ip:ADDR            an address outside the pod network
;;   NS/NAME            a workload class or a pod by name ("*" for any namespace)
;;   NS:SELECTOR        classes in NS whose labels match, e.g. shop:app=web,tier!=batch

(define (parse-cli-selector s)
  (if (member s '("" "*"))
      (selector '() '())
      (let-values ([(ml ex)
                    (for/fold ([ml '()] [ex '()]) ([t (in-list (string-split s ","))])
                      (define term (string-trim t))
                      (cond
                        [(regexp-match #px"^!([^=!]+)$" term) => (lambda (mm) (values ml (cons (list 'DoesNotExist (cadr mm) '()) ex)))]
                        [(regexp-match #px"^([^=!]+)!=(.*)$" term) => (lambda (mm) (values ml (cons (list 'NotIn (cadr mm) (list (caddr mm))) ex)))]
                        [(regexp-match #px"^([^=!]+)==?(.*)$" term) => (lambda (mm) (values (cons (cons (cadr mm) (caddr mm)) ml) ex))]
                        [(regexp-match #px"^([^=!]+)$" term) => (lambda (mm) (values ml (cons (list 'Exists (cadr mm) '()) ex)))]
                        [else (fail-exn "bad selector term ~s" term)]))])
        (selector (reverse ml) (reverse ex)))))

(define (resolve-endpoint m spec)
  (cond
    [(string-prefix? spec "ip:")
     (define text (substring spec 3))
     (define a (parse-ip text))
     (unless a (fail-exn "~s is not a valid IP address" text))
     (list (ext a text))]
    [(regexp-match #px"^([^:/]+):(.*)$" spec)
     => (lambda (mm)
          (define ns (cadr mm))
          (define sel (parse-cli-selector (caddr mm)))
          (filter (lambda (w) (and (or (equal? ns "*") (equal? ns (wl-ns w)))
                                   (selector-matches? sel (wl-labels w))))
                  (model-classes m)))]
    [(regexp-match #px"^([^:/]+)/(.+)$" spec)
     => (lambda (mm)
          (define ns (cadr mm))
          (define name (caddr mm))
          (filter (lambda (w) (and (or (equal? ns "*") (equal? ns (wl-ns w)))
                                   (or (equal? name (wl-name w))
                                       (equal? spec (wl-id w))
                                       (member name (wl-pods w)))))
                  (model-classes m)))]
    [else (fail-exn "bad endpoint ~s: use ip:ADDR, NAMESPACE/NAME or NAMESPACE:key=value" spec)]))

;; ---------------------------------------------------------------------------
;; Audit

(define (classes-in m ns) (filter (lambda (w) (equal? (wl-ns w) ns)) (model-classes m)))

(define (policy-targets m p)
  (filter (lambda (w) (selector-matches? (policy-selector p) (wl-labels w))) (classes-in m (policy-ns p))))

(define (describe-ports r)
  (if (eq? (rule-ports r) 'all) "every port" (string-join (map pspec->string (rule-ports r)) ", ")))

(define (ns-only? p) (and (peer-ns-sel p) (not (peer-pod-sel p)) (not (peer-block p))))
(define (pod-only? p) (and (peer-pod-sel p) (not (peer-ns-sel p)) (not (peer-block p))))

(define (audit-policy m p)
  (define pid (policy-id p))
  (define targets (policy-targets m p))
  (when (null? targets)
    (if (null? (classes-in m (policy-ns p)))
        (add-finding! 'info "dead-policy" pid "namespace ~a has no workloads in this snapshot" (policy-ns p))
        (add-finding! 'warn "dead-policy" pid "podSelector ~a matches no workload, the policy has no effect"
                      (selector->string (policy-selector p)))))
  (when (for/or ([w (in-list targets)]) (wl-host? w))
    (add-finding! 'info "hostnetwork-target" pid
                  "selects hostNetwork workloads (~a), most CNIs do not enforce NetworkPolicy on them"
                  (string-join (for/list ([w (in-list targets)] #:when (wl-host? w)) (wl-id w)) ", ")))
  (for ([r (in-list (append (if (memq 'Ingress (policy-types p)) (policy-ingress p) '())
                            (if (memq 'Egress (policy-types p)) (policy-egress p) '())))])
    (define where (rule-where p r))
    (define ingress? (eq? (rule-dir r) 'Ingress))
    (cond
      [(eq? (rule-peers r) 'all)
       (if ingress?
           (add-finding! 'warn "allow-any-source" where
                         "no from clause: any source, including addresses outside the cluster, may connect on ~a" (describe-ports r))
           (add-finding! 'info "allow-any-destination" where "no to clause: any destination is allowed on ~a" (describe-ports r)))]
      [else
       (define peers (rule-peers r))
       (define key (if ingress? "from" "to"))
       ;; the YAML dash trap: two peers where one was meant
       (for ([a (in-list peers)] [b (in-list (if (null? peers) '() (cdr peers)))])
         (when (or (and (ns-only? a) (pod-only? b)) (and (pod-only? a) (ns-only? b)))
           (add-finding! 'warn "or-not-and" (format "~a.~a[~a..~a]" where key (peer-idx a) (peer-idx b))
                         "namespaceSelector and podSelector are separate peers, so this allows the namespace OR the pods. If you meant pods in that namespace, put both selectors in one list item")))
       (for ([pe (in-list peers)])
         (define pw (format "~a.~a[~a]" where key (peer-idx pe)))
         (cond
           [(peer-block pe)
            (define main (ipblock-main (peer-block pe)))
            (when (and ingress? (= 0 (cidr-len main)) (null? (ipblock-excepts (peer-block pe))))
              (add-finding! 'warn "internet-ingress" pw "ipBlock ~a admits the whole internet on ~a" (cidr-text main) (describe-ports r)))
            (define hits
              (for/list ([w (in-list (model-classes m))]
                         #:when (eq? 'maybe (peer-match m p pe w)))
                (wl-id w)))
            (when (pair? hits)
              (add-finding! 'warn "ipblock-covers-pods" pw
                            "ipBlock ~a covers pod addresses (~a~a). Whether that admits pod traffic depends on the CNI, use selectors for pods"
                            (cidr-text main) (string-join (take hits (min 3 (length hits))) ", ")
                            (if (> (length hits) 3) (format " and ~a more" (- (length hits) 3)) "")))]
           [else
            (when (and (peer-ns-sel pe) (selector-empty? (peer-ns-sel pe)) (not (peer-pod-sel pe)))
              (add-finding! 'info "all-namespaces" pw "namespaceSelector {} admits every pod in every namespace"))
            (unless (for/or ([w (in-list (model-classes m))]) (eq? 'yes (peer-match m p pe w)))
              (add-finding! 'info "dead-peer" pw "matches no workload in this snapshot"))]))])
    ;; named ports that resolve nowhere
    (unless (eq? (rule-ports r) 'all)
      (define candidates
        (if ingress?
            targets
            (if (eq? (rule-peers r) 'all)
                (model-classes m)
                (filter (lambda (w) (for/or ([pe (in-list (rule-peers r))]) (eq? 'yes (peer-match m p pe w))))
                        (model-classes m)))))
      (for ([ps (in-list (rule-ports r))] #:when (string? (pspec-port ps)))
        (unless (for/or ([w (in-list candidates)])
                  (for/or ([cp (in-list (wl-ports w))])
                    (and (equal? (car cp) (pspec-port ps)) (string=? (caddr cp) (pspec-proto ps)))))
          (add-finding! (if (null? candidates) 'info 'warn) "unresolved-named-port" where
                        "named port ~s/~a is not declared by any ~a workload, so this entry matches nothing"
                        (pspec-port ps) (pspec-proto ps) (if ingress? "selected" "destination")))))))

(define (audit-isolation m ignore-ns)
  (define by-ns (make-hash))
  (for ([w (in-list (model-classes m))]
        #:unless (member (wl-ns w) ignore-ns)
        #:when (null? (selecting-policies m w 'Ingress)))
    (hash-update! by-ns (wl-ns w) (lambda (l) (cons (wl-id w) l)) '()))
  (for ([ns (in-list (sort (hash-keys by-ns) string<?))])
    (define ids (reverse (hash-ref by-ns ns)))
    (define total (length (classes-in m ns)))
    (add-finding! (if (member ns SYSTEM-NAMESPACES) 'info 'warn) "no-ingress-isolation" (format "namespace ~a" ns)
                  "~a of ~a workload(s) accept traffic from anywhere because no policy selects them for Ingress: ~a~a"
                  (length ids) total (string-join (take ids (min 5 (length ids))) ", ")
                  (if (> (length ids) 5) (format " and ~a more" (- (length ids) 5)) ""))))

(define (audit-dns m dns-spec dns-ips ignore-ns)
  (define dns-classes (resolve-endpoint m dns-spec))
  (define dns-exts (map (lambda (s) (car (resolve-endpoint m (string-append "ip:" s)))) dns-ips))
  (define targets (append dns-classes dns-exts))
  (cond
    [(null? targets)
     (add-finding! 'info "dns-check-skipped" dns-spec
                   "no DNS workload matched in this snapshot, pass --dns or --dns-ip to check DNS egress")]
    [else
     (for ([w (in-list (model-classes m))]
           #:unless (member (wl-ns w) ignore-ns)
           #:unless (memq w dns-classes)
           #:when (pair? (selecting-policies m w 'Egress)))
       (define (reach proto)
         (for/fold ([acc 'deny]) ([t (in-list targets)])
           (define-values (v _e _i) (pair-eval m w t 53 proto))
           (cond [(eq? acc 'allow) acc] [(eq? v 'allow) 'allow] [(eq? v 'depends) 'depends] [else acc])))
       (define udp (reach "UDP"))
       (define tcp (reach "TCP"))
       (case udp
         [(deny)
          (add-finding! 'error "dns-blocked" (wl-id w)
                        "egress is isolated and no rule allows 53/UDP to ~a, every name lookup will time out"
                        (string-join (map endpoint-name targets) ", "))]
         [(depends)
          (add-finding! 'warn "dns-cni-dependent" (wl-id w)
                        "53/UDP to DNS is only allowed through an ipBlock over pod addresses, which depends on the CNI")]
         [else
          (when (eq? tcp 'deny)
            (add-finding! 'warn "dns-tcp-blocked" (wl-id w)
                          "53/UDP is allowed but 53/TCP is not, truncated DNS answers will fail to retry over TCP"))]))]))

(define (run-audit m #:dns [dns-spec DEFAULT-DNS-SPEC] #:dns-ips [dns-ips '()] #:ignore-ns [ignore-ns '()])
  (for ([p (in-list (model-policies m))]) (audit-policy m p))
  (audit-isolation m ignore-ns)
  (audit-dns m dns-spec dns-ips ignore-ns))

;; ---------------------------------------------------------------------------
;; Expectations

(struct expectation (line verb src-spec dst-spec port proto) #:transparent)
(struct outcome (exp status detail) #:transparent)

(define (parse-expectations text)
  (for*/list ([(raw n) (in-parallel (in-list (string-split text "\n" #:trim? #f)) (in-naturals))]
              [line (in-value (string-trim (car (string-split (string-append raw " ") "#" #:trim? #f))))]
              #:unless (string=? line ""))
    (match (string-split line)
      [(list verb src "->" dst port)
       #:when (member verb '("allow" "deny"))
       (define pp (parse-port-arg port))
       (expectation (add1 n) (string->symbol verb) src dst (car pp) (cdr pp))]
      [_ (fail-exn "expectations line ~a: expected `allow|deny SRC -> DST PORT/PROTO`, got ~s" (add1 n) line)])))

(define (run-check m exps strict?)
  (for/list ([e (in-list exps)])
    (define srcs (resolve-endpoint m (expectation-src-spec e)))
    (define dsts (resolve-endpoint m (expectation-dst-spec e)))
    (cond
      [(or (null? srcs) (null? dsts))
       (outcome e 'fail (format "~a matched nothing, an expectation that tests nothing is a broken test"
                                (if (null? srcs) (expectation-src-spec e) (expectation-dst-spec e))))]
      [else
       (define results
         (for*/list ([s (in-list srcs)] [d (in-list dsts)] #:when (testable-pair? s d))
           (define-values (v eg in) (pair-eval m s d (expectation-port e) (expectation-proto e)))
           (list s d v eg in)))
       (define want (if (eq? (expectation-verb e) 'allow) 'allow 'deny))
       (define bad (filter (lambda (r) (not (memq (caddr r) (list want 'depends)))) results))
       (define unsure (filter (lambda (r) (eq? (caddr r) 'depends)) results))
       (define (show r)
         (match-define (list s d v eg in) r)
         (format "~a -> ~a is ~a [egress: ~a] [ingress: ~a]"
                 (endpoint-name s) (endpoint-name d) v
                 (string-join (side-witnesses eg) "; ") (string-join (side-witnesses in) "; ")))
       (cond
         [(null? results) (outcome e 'fail "source and destination are the same single workload, nothing to test")]
         [(pair? bad) (outcome e 'fail (string-join (map show (take bad (min 5 (length bad)))) "\n      "))]
         [(pair? unsure) (outcome e (if strict? 'fail 'unsure) (string-join (map show (take unsure (min 5 (length unsure)))) "\n      "))]
         [else (outcome e 'pass (format "~a pair(s) checked" (length results)))])])))

;; ---------------------------------------------------------------------------
;; Rendering

(define (finding->jsexpr f)
  (hasheq 'severity (symbol->string (finding-severity f)) 'code (finding-code f)
          'subject (finding-subject f) 'message (finding-message f)))

(define (side->jsexpr s)
  (hasheq 'direction (symbol->string (side-dir s)) 'verdict (symbol->string (side-verdict s))
          'policies (map policy-id (side-policies s)) 'because (side-witnesses s)))

(define (sorted-findings fs)
  (sort (remove-duplicates fs)
        (lambda (a b)
          (define ra (severity-rank (finding-severity a)))
          (define rb (severity-rank (finding-severity b)))
          (or (< ra rb)
              (and (= ra rb)
                   (or (string<? (finding-code a) (finding-code b))
                       (and (string=? (finding-code a) (finding-code b))
                            (string<? (finding-subject a) (finding-subject b)))))))))

(define (model-summary m)
  (define ing (for/sum ([w (in-list (model-classes m))]) (if (pair? (selecting-policies m w 'Ingress)) 1 0)))
  (define egr (for/sum ([w (in-list (model-classes m))]) (if (pair? (selecting-policies m w 'Egress)) 1 0)))
  (hasheq 'namespaces (hash-count (model-namespaces m))
          'workloads (length (model-classes m))
          'policies (length (model-policies m))
          'ingressIsolated ing
          'egressIsolated egr))

(define (print-findings fs)
  (for ([f (in-list fs)])
    (printf "[~a] ~a  ~a\n    ~a\n"
            (string-upcase (symbol->string (finding-severity f)))
            (finding-code f) (finding-subject f) (finding-message f))))

(define (print-summary s)
  (printf "~a namespaces, ~a workloads, ~a policies, ~a workloads isolated for ingress, ~a for egress\n"
          (hash-ref s 'namespaces) (hash-ref s 'workloads) (hash-ref s 'policies)
          (hash-ref s 'ingressIsolated) (hash-ref s 'egressIsolated)))

(define (fails-gate? fs fail-on)
  (and (not (eq? fail-on 'none))
       (for/or ([f (in-list fs)])
         (<= (severity-rank (finding-severity f)) (severity-rank fail-on)))))

;; ---------------------------------------------------------------------------
;; CLI

(define (load-inputs files)
  (define docs
    (if (null? files)
        (read-json-documents (current-input-port) "stdin")
        (append-map (lambda (f)
                      (if (equal? f "-")
                          (read-json-documents (current-input-port) "stdin")
                          (begin
                            (unless (file-exists? f) (fail-exn "~a: no such file" f))
                            (call-with-input-file f (lambda (in) (read-json-documents in f))))))
                    files)))
  (objects-from-jsexprs docs))

(define (main argv)
  (define files '())
  (define default-ns "default")
  (define pod-cidr-texts '())
  (define dns-spec DEFAULT-DNS-SPEC)
  (define dns-ips '())
  (define ignore-ns '())
  (define fail-on 'error)
  (define fmt 'text)
  (define strict? #f)
  (define limit 50)
  (define-values (command rest)
    (command-line
     #:program "NetworkPolicyReachabilityAuditor.rkt"
     #:argv argv
     #:usage-help
     "Commands:"
     "  audit                         lint policies and report isolation gaps"
     "  explain SRC DST PORT[/PROTO]  show the verdict and the deciding rule for each pair"
     "  check FILE                    gate on allow/deny expectations"
     "  matrix PORT[/PROTO]           list every workload pair that can connect"
     "Endpoints: ip:ADDR | NAMESPACE/NAME | NAMESPACE:key=value,key!=value,!key (NAMESPACE may be *)"
     #:multi
     [("-f" "--file") path "JSON input: kubectl -o json output, a List, or rendered manifests (repeatable, - for stdin)"
                      (set! files (append files (list path)))]
     [("--pod-cidr") c "Cluster pod CIDR, used to flag ipBlocks that cover template workloads (repeatable)"
                     (set! pod-cidr-texts (append pod-cidr-texts (list c)))]
     [("--dns-ip") ip "Extra DNS address to accept in the DNS check, e.g. a NodeLocal DNSCache IP (repeatable)"
                   (set! dns-ips (append dns-ips (list ip)))]
     [("--ignore-namespace") ns "Skip this namespace in isolation and DNS findings (repeatable)"
                             (set! ignore-ns (append ignore-ns (list ns)))]
     #:once-each
     [("-n" "--namespace") ns "Namespace for objects that do not set one (default: default)" (set! default-ns ns)]
     [("--dns") spec "Endpoint spec for cluster DNS (default: kube-system:k8s-app=kube-dns)" (set! dns-spec spec)]
     [("--fail-on") level "audit exits 1 at this severity or worse: error, warn, info or none (default: error)"
                    (set! fail-on (string->symbol level))
                    (unless (memq fail-on '(error warn info none)) (fail-exn "--fail-on must be error, warn, info or none"))]
     [("--format") f "text or json (default: text)"
                   (set! fmt (string->symbol f))
                   (unless (memq fmt '(text json)) (fail-exn "--format must be text or json"))]
     [("--strict") "check: treat CNI dependent verdicts as failures" (set! strict? #t)]
     [("--limit") n "explain: maximum pairs to print (default: 50)"
                  (set! limit (or (string->number n) (fail-exn "--limit must be a number")))]
     #:args (command . rest)
     (values command rest)))
  (define pod-cidrs
    (for/list ([t (in-list pod-cidr-texts)])
      (or (parse-cidr t) (fail-exn "--pod-cidr ~s is not a valid CIDR" t))))
  (define fbox (box '()))
  (parameterize ([current-findings fbox]
                 [current-selecting-cache (make-selecting-cache)])
    (define m (build-model (load-inputs files) #:default-namespace default-ns #:pod-cidrs pod-cidrs))
    (define summary (model-summary m))
    (match (cons command rest)
      [(list "audit")
       (run-audit m #:dns dns-spec #:dns-ips dns-ips #:ignore-ns ignore-ns)
       (define fs (sorted-findings (unbox fbox)))
       (if (eq? fmt 'json)
           (begin (write-json (hasheq 'summary summary 'findings (map finding->jsexpr fs))) (newline))
           (begin (print-summary summary)
                  (when (null? fs) (printf "no findings\n"))
                  (print-findings fs)))
       (if (fails-gate? fs fail-on) 1 0)]
      [(list "explain" src dst port)
       (define pp (parse-port-arg port))
       (define srcs (resolve-endpoint m src))
       (define dsts (resolve-endpoint m dst))
       (when (null? srcs) (fail-exn "source ~s matched no workload" src))
       (when (null? dsts) (fail-exn "destination ~s matched no workload" dst))
       (define pairs (for*/list ([s (in-list srcs)] [d (in-list dsts)] #:when (testable-pair? s d)) (cons s d)))
       (define rows
         (for/list ([pr (in-list (take pairs (min limit (length pairs))))])
           (define-values (v eg in) (pair-eval m (car pr) (cdr pr) (car pp) (cdr pp)))
           (list (car pr) (cdr pr) v eg in)))
       (cond
         [(eq? fmt 'json)
          (write-json
           (for/list ([r (in-list rows)])
             (match-define (list s d v eg in) r)
             (hasheq 'source (endpoint-name s) 'destination (endpoint-name d)
                     'port (car pp) 'protocol (cdr pp) 'verdict (symbol->string v)
                     'egress (side->jsexpr eg) 'ingress (side->jsexpr in))))
          (newline)]
         [else
          (for ([r (in-list rows)])
            (match-define (list s d v eg in) r)
            (printf "~a -> ~a ~a/~a: ~a\n" (endpoint-name s) (endpoint-name d) (car pp) (cdr pp)
                    (string-upcase (symbol->string v)))
            (for ([sd (list eg in)])
              (printf "  ~a (~a)\n" (string-downcase (symbol->string (side-dir sd))) (side-verdict sd))
              (for ([w (in-list (side-witnesses sd))]) (printf "    ~a\n" w))))
          (when (> (length pairs) limit)
            (printf "... ~a more pair(s), raise --limit to see them\n" (- (length pairs) limit)))])
       (if (for/or ([r (in-list rows)]) (eq? (caddr r) 'deny)) 1 0)]
      [(list "check" path)
       (unless (file-exists? path) (fail-exn "~a: no such file" path))
       (define exps (parse-expectations (file->string* path)))
       (define outs (run-check m exps strict?))
       (cond
         [(eq? fmt 'json)
          (write-json
           (for/list ([o (in-list outs)])
             (define e (outcome-exp o))
             (hasheq 'line (expectation-line e) 'expect (symbol->string (expectation-verb e))
                     'source (expectation-src-spec e) 'destination (expectation-dst-spec e)
                     'port (expectation-port e) 'protocol (expectation-proto e)
                     'status (symbol->string (outcome-status o)) 'detail (outcome-detail o))))
          (newline)]
         [else
          (for ([o (in-list outs)])
            (define e (outcome-exp o))
            (printf "~a  line ~a: ~a ~a -> ~a ~a/~a\n      ~a\n"
                    (string-upcase (symbol->string (outcome-status o))) (expectation-line e)
                    (expectation-verb e) (expectation-src-spec e) (expectation-dst-spec e)
                    (expectation-port e) (expectation-proto e) (outcome-detail o)))
          (printf "~a passed, ~a failed, ~a CNI dependent\n"
                  (count (lambda (o) (eq? (outcome-status o) 'pass)) outs)
                  (count (lambda (o) (eq? (outcome-status o) 'fail)) outs)
                  (count (lambda (o) (eq? (outcome-status o) 'unsure)) outs))])
       (if (for/or ([o (in-list outs)]) (eq? (outcome-status o) 'fail)) 1 0)]
      [(list "matrix" port)
       (define pp (parse-port-arg port))
       (define edges
         (for*/list ([s (in-list (model-classes m))]
                     [d (in-list (model-classes m))]
                     #:when (testable-pair? s d)
                     [v (in-value (let-values ([(v _e _i) (pair-eval m s d (car pp) (cdr pp))]) v))]
                     #:unless (eq? v 'deny))
           (list (wl-id s) (wl-id d) v)))
       (cond
         [(eq? fmt 'json)
          (write-json (hasheq 'port (car pp) 'protocol (cdr pp)
                              'edges (for/list ([e (in-list edges)])
                                       (hasheq 'source (car e) 'destination (cadr e) 'verdict (symbol->string (caddr e))))))
          (newline)]
         [else
          (for ([e (in-list edges)])
            (printf "~a -> ~a  ~a\n" (car e) (cadr e) (if (eq? (caddr e) 'allow) "allow" "depends on CNI")))
          (define n (length (model-classes m)))
          (printf "~a of ~a directed workload pairs can connect on ~a/~a\n" (length edges) (* n n) (car pp) (cdr pp))])
       0]
      [_ (fail-exn "unknown command or wrong arguments: ~a" (string-join (cons command rest) " "))])))

(define (file->string* path) (call-with-input-file path port->string))

(module+ main
  (define code
    (with-handlers ([exn:fail:user?
                     (lambda (e) (eprintf "error: ~a\n" (exn-message e)) 2)]
                    [exn:fail?
                     (lambda (e) (eprintf "internal error: ~a\n" (exn-message e)) 2)])
      (main (current-command-line-arguments))))
  (exit code))
