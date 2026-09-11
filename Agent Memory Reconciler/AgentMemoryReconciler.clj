(ns agent-memory-reconciler
  "Delta-state CRDT document store for shared AI agent memory.

  Multiple agent replicas (swarm workers, offline-capable edge devices, or
  independent sessions of the same assistant) each hold a local copy of a
  MemoryDocument and mutate it while disconnected from each other. merge-doc
  joins any two replicas' documents into the state both would reach no
  matter what order the merges happen in, with no coordinator and no lost
  writes: the join-semilattice construction from Shapiro, Preguica, Baquero
  and Zawirski, 'A comprehensive study of Convergent and Commutative
  Replicated Data Types' (2011), and the bandwidth-trimming delta technique
  from Almeida, Shoker and Baquero, 'Delta State Replicated Data Types'
  (2018)."
  (:require [clojure.edn :as edn]
            [clojure.set :as set]))

;; -----------------------------------------------------------------------
;; AWORSet: Add-Wins Observed-Remove Set
;;
;; :adds maps every tag ever added to its value. A tag is [replica counter],
;; unique because each replica only ever hands out its own strictly
;; increasing counters. :tombs maps every tombstoned tag to the removal-dot
;; that killed it, [remover-replica remover-counter], its OWN monotonic
;; stream separate from the add counters. A value is "in" the set while at
;; least one of its tags is in :adds and not in :tombs.
;;
;; Removing only tombstones tags this replica currently observes for that
;; value (the "observed remove" rule), so a concurrent add nobody has
;; merged in yet survives: that is what "add-wins" means, and it is the
;; right default for shared agent memory, where silently losing a fact
;; another agent just discovered is worse than a stale duplicate.
;;
;; Keeping both counters explicit, instead of collapsing removal into "the
;; add's own counter is covered but missing from elements", is a deliberate
;; choice. That collapsed representation is the textbook compression for
;; add-only G-Sets, but it silently corrupts an OR-Set the moment a replica
;; has two dots from the same source with one live and one tombstoned in
;; between: a delta trimmed against a borrowed causal watermark can then
;; make an unrelated, still-live dot look removed. Giving removals their
;; own per-replica counter avoids that trap entirely: merge here is a plain
;; monotonic map union, which is unconditionally commutative, associative
;; and idempotent, and delta trimming is unconditionally safe because
;; omitting a key from a grow-only map never implies anything about keys
;; that were not sent.
;; -----------------------------------------------------------------------

(defn empty-set [] {:adds {} :tombs {} :add-seq {} :remove-seq {}})

(defn set-add
  [aworset value replica]
  (let [c (inc (long (get (:add-seq aworset) replica 0)))
        tag [replica c]]
    (-> aworset
        (assoc-in [:adds tag] value)
        (assoc-in [:add-seq replica] c))))

(defn set-remove
  "Tombstones every tag replica currently observes for value. A no-op if
  the value is not currently visible to replica, which is the correct
  add-wins outcome for a remove racing a not-yet-delivered concurrent add."
  [aworset value replica]
  (let [live-tags (into [] (comp (filter (fn [[tag v]] (and (= v value) (not (contains? (:tombs aworset) tag)))))
                                  (map key))
                         (:adds aworset))]
    (if (empty? live-tags)
      aworset
      (let [c (inc (long (get (:remove-seq aworset) replica 0)))
            removal-dot [replica c]]
        (-> aworset
            (update :tombs into (map (fn [tag] [tag removal-dot])) live-tags)
            (assoc-in [:remove-seq replica] c))))))

(defn set-view
  [aworset]
  (into #{} (vals (apply dissoc (:adds aworset) (keys (:tombs aworset))))))

(defn merge-set
  "The AWORSet join: a plain union of two grow-only maps per side (adds,
  tombs) plus a pointwise max of the two generation-counter maps. Nothing
  here is conditional on the other side's state, which is what makes it
  trivially commutative, associative and idempotent."
  [a b]
  {:adds (merge (:adds a) (:adds b))
   :tombs (merge (:tombs a) (:tombs b))
   :add-seq (merge-with max (:add-seq a) (:add-seq b))
   :remove-seq (merge-with max (:remove-seq a) (:remove-seq b))})

(defn set-delta-since
  "A delta AWORSet holding only the adds and tombstones peer-cursor has not
  seen yet, identified by comparing each tag's own generation counter
  against the cursor's per-replica watermark. Because merge-set is a plain
  union, this trimming can never manufacture a false removal the way
  ctx-implied-absence schemes can: an omitted key just means 'peer already
  has this exact entry', never 'this entry is gone'."
  [aworset peer-cursor]
  (let [add-seq (:add-seq peer-cursor {})
        remove-seq (:remove-seq peer-cursor {})]
    {:adds (into {} (filter (fn [[[r c] _]] (> (long c) (long (get add-seq r 0))))) (:adds aworset))
     :tombs (into {} (filter (fn [[_ [rr rc]]] (> (long rc) (long (get remove-seq rr 0))))) (:tombs aworset))
     :add-seq (:add-seq aworset)
     :remove-seq (:remove-seq aworset)}))

(defn gc-tombstones
  "Purges tombstoned tags from both :adds and :tombs once stable-remove-seq
  proves every replica currently believed live has already merged that
  removal. stable-remove-seq must be the true pointwise minimum of
  :remove-seq across every such replica (see pointwise-min-cursor): passing
  anything less conservative risks the exact tombstone-resurrection hazard
  Cassandra and Riak operators guard against with a GC grace period, where
  a replica that was partitioned past the cutoff re-merges a value
  everyone else already agreed to delete."
  [aworset stable-remove-seq]
  (let [stale? (fn [[_ [rr rc]]] (<= (long rc) (long (get stable-remove-seq rr 0))))
        doomed-tags (keys (into {} (filter stale?) (:tombs aworset)))]
    (-> aworset
        (update :adds #(apply dissoc % doomed-tags))
        (update :tombs #(apply dissoc % doomed-tags)))))

(defn pointwise-min-cursor
  "The pointwise minimum of several :remove-seq (or :add-seq) maps, treating
  a replica missing from one map as 0. This is the causal-stability
  watermark gc-tombstones needs: the highest removal counter every replica
  in cursors is guaranteed to have already observed."
  [cursors]
  (let [ks (into #{} (mapcat keys) cursors)]
    (into {} (map (fn [k] [k (reduce min (map #(get % k 0) cursors))])) ks)))

;; -----------------------------------------------------------------------
;; LWWRegister: last-write-wins scalar
;;
;; ts is [logical-clock replica]. Higher logical-clock wins; a tie is
;; broken by comparing replica ids so every replica computes the identical
;; winner without talking to each other first, the deterministic-tiebreak
;; LWW-Register from Shapiro et al. 2011, section 3.3.
;; -----------------------------------------------------------------------

(def unset-register {:value nil :ts [0 ""]})

(defn reg-set [_register value replica logical-clock] {:value value :ts [logical-clock replica]})

(defn reg-view [register] (:value register))

(defn- ts-greater?
  [[c1 r1] [c2 r2]]
  (or (> (long c1) (long c2)) (and (= c1 c2) (pos? (compare (str r1) (str r2))))))

(defn merge-reg [a b] (if (ts-greater? (:ts b) (:ts a)) b a))

;; -----------------------------------------------------------------------
;; PNCounter: increment/decrement counter
;;
;; :p and :n are per-replica running totals, each only ever growing
;; locally, so the merge is a pointwise max exactly like a G-Counter, and
;; the counter's value is (sum p) - (sum n). Standard PN-Counter from
;; Shapiro et al. 2011, section 3.1.
;; -----------------------------------------------------------------------

(def zero-counter {:p {} :n {}})

(defn counter-inc
  [counter replica amount]
  {:pre [(not (neg? amount))]}
  (update-in counter [:p replica] (fnil + 0) amount))

(defn counter-dec
  [counter replica amount]
  {:pre [(not (neg? amount))]}
  (update-in counter [:n replica] (fnil + 0) amount))

(defn counter-view
  [counter]
  (- (reduce + 0 (vals (:p counter))) (reduce + 0 (vals (:n counter)))))

(defn merge-counter [a b] {:p (merge-with max (:p a) (:p b)) :n (merge-with max (:n a) (:n b))})

;; -----------------------------------------------------------------------
;; MemoryDocument: the shared agent-memory record
;;
;; :facts  - discovered facts / observations, an AWORSet of strings
;; :tasks  - open work items, an AWORSet of strings
;; :status - the swarm's current phase, an LWWRegister
;; :budget - tokens or dollars spent so far, a PNCounter
;;
;; Every field is independently a CRDT, and a product of CRDTs merged
;; fieldwise is itself a CRDT (the join-semilattice product construction),
;; so merge-doc needs no locking or field ordering.
;; -----------------------------------------------------------------------

(defn empty-document [] {:facts (empty-set) :tasks (empty-set) :status unset-register :budget zero-counter})

(defn add-fact [doc fact replica] (update doc :facts set-add fact replica))
(defn remove-fact [doc fact replica] (update doc :facts set-remove fact replica))
(defn add-task [doc task replica] (update doc :tasks set-add task replica))
(defn complete-task [doc task replica] (update doc :tasks set-remove task replica))
(defn set-status [doc status replica logical-clock] (update doc :status reg-set status replica logical-clock))
(defn spend-budget [doc replica amount] (update doc :budget counter-inc replica amount))
(defn refund-budget [doc replica amount] (update doc :budget counter-dec replica amount))

(defn doc-view
  [doc]
  {:facts (set-view (:facts doc))
   :tasks (set-view (:tasks doc))
   :status (reg-view (:status doc))
   :budget (counter-view (:budget doc))})

(defn- set-diff-summary
  [field before after]
  (let [b (set-view before) a (set-view after)]
    (when (not= b a)
      {:field field :added (set/difference a b) :removed (set/difference b a)})))

(defn- status-change-summary
  [before after]
  (when (not= (:ts before) (:ts after))
    {:field :status :from (:value before) :to (:value after)
     :reason (str "timestamp " (:ts after) " beat " (:ts before))}))

(defn- budget-change-summary
  [before after]
  (let [bv (counter-view before) av (counter-view after)]
    (when (not= bv av)
      {:field :budget :from bv :to av :delta (- av bv)})))

(defn merge-doc
  "Joins two MemoryDocument replicas into the state both would converge to.
  Commutative, associative and idempotent (see check-sec-laws below), so it
  is safe to call in any order, any number of times, on full states or on
  deltas produced by delta-since. Returns {:doc merged :changes [...]}
  where changes is an audit trail explaining what the merge actually
  changed and, for the status register, why one write beat the other."
  [doc-a doc-b]
  (let [facts' (merge-set (:facts doc-a) (:facts doc-b))
        tasks' (merge-set (:tasks doc-a) (:tasks doc-b))
        status' (merge-reg (:status doc-a) (:status doc-b))
        budget' (merge-counter (:budget doc-a) (:budget doc-b))
        merged {:facts facts' :tasks tasks' :status status' :budget budget'}
        changes (vec (keep identity
                            [(set-diff-summary :facts (:facts doc-a) facts')
                             (set-diff-summary :tasks (:tasks doc-a) tasks')
                             (status-change-summary (:status doc-a) status')
                             (budget-change-summary (:budget doc-a) budget')]))]
    {:doc merged :changes changes}))

(defn merge-many
  "Folds merge-doc across any number of replicas. Order does not matter."
  [docs]
  (reduce (fn [acc doc] (:doc (merge-doc acc doc))) (empty-document) docs))

(defn peer-cursor
  "The generation counters a peer should hand back on its next sync request
  so this replica can compute a minimal delta-since instead of resending
  everything it already has."
  [doc]
  {:facts {:add-seq (:add-seq (:facts doc)) :remove-seq (:remove-seq (:facts doc))}
   :tasks {:add-seq (:add-seq (:tasks doc)) :remove-seq (:remove-seq (:tasks doc))}})

(defn delta-since
  "A delta MemoryDocument holding only the facts/tasks tags cursor has not
  observed. status and budget are already O(1) and O(replica-count), so
  they always travel in full: trimming them would not shrink the wire
  payload and would only complicate the merge."
  [doc cursor]
  {:facts (set-delta-since (:facts doc) (:facts cursor {}))
   :tasks (set-delta-since (:tasks doc) (:tasks cursor {}))
   :status (:status doc)
   :budget (:budget doc)})

;; -----------------------------------------------------------------------
;; Wire format
;;
;; A MemoryDocument is already plain EDN-able data: nested maps and sets of
;; keywords, strings, numbers and vectors. Vectors as map keys ([replica
;; counter] tags) are native EDN, so no custom tagged-literal readers or
;; record flattening are needed to round-trip it.
;; -----------------------------------------------------------------------

(defn doc->edn [doc] (pr-str doc))
(defn edn->doc [s] (edn/read-string s))

;; -----------------------------------------------------------------------
;; Strong Eventual Consistency law checker
;;
;; A merge function only qualifies as a CRDT join if it is commutative,
;; associative and idempotent: Shapiro et al. 2011, theorem 1, is exactly
;; the claim that those three properties guarantee every replica converges
;; on the same state regardless of message order, duplication or delay.
;; Rather than asserting that in a comment, this checks it against the
;; actual documents a run produced.
;; -----------------------------------------------------------------------

(defn- docs-equal? [a b] (= (doc-view a) (doc-view b)))

(defn check-sec-laws
  "Verifies merge-doc is commutative, associative and idempotent over every
  pair and triple drawn from docs. Returns true or throws ex-info naming
  the law that failed and the inputs that broke it."
  [docs]
  (doseq [a docs, b docs]
    (let [ab (:doc (merge-doc a b))
          ba (:doc (merge-doc b a))]
      (when-not (docs-equal? ab ba)
        (throw (ex-info "merge-doc is not commutative" {:a (doc-view a) :b (doc-view b)})))))
  (doseq [a docs, b docs, c docs]
    (let [left (:doc (merge-doc (:doc (merge-doc a b)) c))
          right (:doc (merge-doc a (:doc (merge-doc b c))))]
      (when-not (docs-equal? left right)
        (throw (ex-info "merge-doc is not associative"
                         {:a (doc-view a) :b (doc-view b) :c (doc-view c)})))))
  (doseq [d docs]
    (when-not (docs-equal? d (:doc (merge-doc d d)))
      (throw (ex-info "merge-doc is not idempotent" {:d (doc-view d)}))))
  true)

;; -----------------------------------------------------------------------
;; Demo: three agents, a network partition, delta sync and GC
;; -----------------------------------------------------------------------

(defn -main
  [& _]
  (let [d0 (empty-document)

        ;; Agent A and Agent B both go offline from the same baseline and
        ;; work independently: the same fact discovered twice, different
        ;; tasks, different status opinions, both spending budget nobody
        ;; else can see yet.
        da (-> d0
               (add-fact "image-gen cost exceeds daily cap" :agent-a)
               (add-task "review-budget" :agent-a)
               (set-status "drafting" :agent-a 1)
               (spend-budget :agent-a 120))
        db (-> d0
               (add-fact "image-gen cost exceeds daily cap" :agent-b)
               (add-task "notify-oncall" :agent-b)
               (set-status "researching" :agent-b 1)
               (spend-budget :agent-b 340))

        {ab :doc changes-ab :changes} (merge-doc da db)

        ;; Agent C comes online, syncs with the merged a+b state, then
        ;; independently completes review-budget and raises the status
        ;; clock to 3. At the very same moment, Agent A, still offline and
        ;; unaware C exists, also raises its own status clock to 3. Both
        ;; writes are genuinely concurrent.
        dc (-> ab
               (complete-task "review-budget" :agent-c)
               (set-status "reviewing" :agent-c 3))
        da2 (set-status da "blocked" :agent-a 3)

        {final :doc changes-final :changes} (merge-doc da2 dc)]

    (println "== after merging Agent A and Agent B ==")
    (prn (doc-view ab))
    (doseq [c changes-ab] (prn c))

    (println "\n== after reconciling Agent A's continued offline edits with Agent C ==")
    (prn (doc-view final))
    (doseq [c changes-final] (prn c))
    (println (str "\nstatus tie between agent-a@3 and agent-c@3 resolved to "
                   (pr-str (reg-view (:status final)))
                   " by replica-id tiebreak"))

    (println "\n== verifying strong eventual consistency laws on this run's states ==")
    (println (check-sec-laws [d0 da db ab dc da2 final]))

    (println "\n== round trip through the EDN wire format ==")
    (println (= (doc-view final) (doc-view (edn->doc (doc->edn final)))))

    (println "\n== delta sync: catching Agent B up to final without resending everything ==")
    (let [b-cursor (peer-cursor db)
          delta (delta-since final b-cursor)
          b-synced (:doc (merge-doc db delta))]
      (println "tags B was missing:" (count (:adds (:facts delta))) "fact adds,"
                (count (:adds (:tasks delta))) "task adds,"
                (count (:tombs (:tasks delta))) "task tombstones")
      (println "B after applying the delta matches final:" (docs-equal? b-synced final)))

    (println "\n== tombstone GC: purging the review-budget removal once every replica has it ==")
    (let [stable (pointwise-min-cursor [(:remove-seq (:tasks da2)) (:remove-seq (:tasks dc)) (:remove-seq (:tasks final))])
          before (count (:tombs (:tasks final)))
          gcd (gc-tombstones (:tasks final) stable)
          after (count (:tombs (:tasks gcd)))]
      (println "tombstones before GC:" before "after GC:" after)
      (println "materialized view unchanged by GC:" (= (set-view (:tasks final)) (set-view gcd))))))

(-main)
