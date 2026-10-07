(ns hourly-energy-matching-ledger
  "Hourly (24/7) clean energy matching ledger.

  Takes metered load, energy attribute certificates and grid emission
  factors, and answers the question an annual REC claim hides: in which
  intervals was the load really covered by deliverable clean supply?

  Matching is an exact maximum flow (Dinic) over a bipartite graph of
  supply nodes (region, interval) and load nodes (region, interval). Edges
  exist only where a deliverability policy and a time window allow the
  certificate to serve the load. Every certificate Wh is used at most once,
  every load Wh is covered at most once, and the total matched energy is
  the true optimum, not a greedy approximation. Tiered edge activation
  makes same region, same interval pairings win ties, so the allocation
  list reads like a human would expect.

  All energy is held as integer Wh (long) so the flow arithmetic is exact
  and the output is byte for byte reproducible."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.nio.file Files Paths StandardCopyOption]
           [java.time Instant OffsetDateTime]
           [java.time.format DateTimeParseException]
           [java.util Locale]))

;; -----------------------------------------------------------------------
;; Constants

(def valid-intervals
  "Interval lengths in minutes that divide an hour evenly."
  #{1 5 10 15 20 30 60})

(def max-kwh-per-row 1.0e9)
(def max-g-per-kwh 5000.0)
(def max-flow-edges
  "Hard cap on supply to load edges. A wide window over a year of data can
  explode quadratically, so we refuse instead of exhausting the heap."
  4000000)
(def inf-cap (quot Long/MAX_VALUE 4))
(def default-policy
  {:interval-minutes 60
   :window-before 0
   :window-after 0
   :deliverability {}
   :residual-fallback :grid})

(def usage-text
  "Usage: clojure -M HourlyEnergyMatchingLedger.clj --load FILE --supply FILE --grid FILE [options]

Required:
  --load FILE             CSV: hour,region,kwh[,meter]
  --supply FILE           CSV: certificate_id,region,hour,kwh[,asset,g_per_kwh]
  --grid FILE             CSV: hour,region,grid_g_per_kwh[,residual_g_per_kwh]

Options:
  --policy FILE           EDN policy (interval, windows, deliverability)
  --interval-minutes N    1 5 10 15 20 30 or 60 (default 60)
  --window-before N       supply may be generated up to N intervals before the load
  --window-after N        supply may be generated up to N intervals after the load
  --min-cfe X             fail with exit 1 when overall hourly CFE is below X (0 to 1)
  --max-rejects N         tolerated rejected input rows before exit 3 (default 0)
  --format json|text      report format on stdout (default json)
  --hourly-out FILE       write the per interval table as CSV
  --alloc-out FILE        write the certificate allocation list as CSV
  --help                  show this text

Exit codes: 0 pass, 1 CFE below threshold, 2 usage or fatal input error,
3 more rejected rows than allowed.")

;; -----------------------------------------------------------------------
;; Errors

(defn- fatal [msg & [data]]
  (throw (ex-info msg (merge {:fatal true} data))))

(defn- fmt ^String [^String f & args]
  (String/format Locale/ROOT f (to-array args)))

;; -----------------------------------------------------------------------
;; CSV reader: RFC 4180 quoting, BOM, CRLF, embedded newlines.

(defn- finish-field [^StringBuilder sb quoted?]
  (let [s (str sb)] (if quoted? s (str/trim s))))

(defn parse-csv
  "Parses CSV text into a vector of {:line n :cells [...]} records. Blank
  records are kept (the caller filters them). Throws on an unterminated
  quoted field instead of swallowing the rest of the file into one cell."
  [^String text]
  (let [n (.length text)
        rows (java.util.ArrayList.)]
    (loop [i (if (and (pos? n) (= (int (.charAt text 0)) 0xFEFF)) 1 0)
           sb (StringBuilder.) cells [] q? false quoted? false line 1 rec 1]
      (if (>= i n)
        (do (when q? (fatal (str "unterminated quoted field starting on line " rec)))
            (let [cells (if (or (pos? (.length sb)) quoted? (seq cells))
                          (conj cells (finish-field sb quoted?))
                          cells)]
              (when (seq cells) (.add rows {:line rec :cells cells})))
            (vec rows))
        (let [c (.charAt text i)]
          (cond
            q? (cond
                 (and (= c \") (< (inc i) n) (= (.charAt text (inc i)) \"))
                 (recur (+ i 2) (.append sb \") cells true quoted? line rec)

                 (= c \")
                 (recur (inc i) sb cells false quoted? line rec)

                 :else
                 (recur (inc i) (.append sb c) cells true quoted?
                        (if (= c \newline) (inc line) line) rec))

            (= c \") (recur (inc i) sb cells true true line rec)

            (= c \,) (recur (inc i) (StringBuilder.) (conj cells (finish-field sb quoted?))
                            false false line rec)

            (or (= c \newline) (= c \return))
            (let [i' (if (and (= c \return) (< (inc i) n) (= (.charAt text (inc i)) \newline))
                       (+ i 2) (inc i))
                  cells' (if (or (pos? (.length sb)) quoted? (seq cells))
                           (conj cells (finish-field sb quoted?))
                           cells)]
              (when (seq cells') (.add rows {:line rec :cells cells'}))
              (recur i' (StringBuilder.) [] false false (inc line) (inc line)))

            :else (recur (inc i) (.append sb c) cells false quoted? line rec)))))))

(defn read-table
  "Reads a CSV file into {:records [{:line n :m {header value}}] :rejects [...]}.
  Header names are lower cased. A missing required column is fatal because
  every row would be wrong. A short or long row is rejected individually."
  [label file required]
  (let [text (try (slurp file)
                  (catch java.io.IOException e
                    (fatal (str label ": cannot read " file ": " (.getMessage e)))))
        rows (->> (parse-csv text)
                  (remove (fn [{:keys [cells]}] (every? str/blank? cells))))]
    (when (empty? rows) (fatal (str label ": file is empty: " file)))
    (let [header (mapv (comp str/lower-case str/trim) (:cells (first rows)))
          dupes (->> (frequencies header) (filter (fn [[_ c]] (> c 1))) (map key))]
      (when (seq dupes) (fatal (str label ": duplicate header columns: " (str/join ", " dupes))))
      (let [missing (remove (set header) required)]
        (when (seq missing)
          (fatal (str label ": missing required columns: " (str/join ", " missing)
                      " (found: " (str/join ", " header) ")"))))
      (reduce
       (fn [acc {:keys [line cells]}]
         (if (= (count cells) (count header))
           (update acc :records conj {:line line :m (zipmap header cells)})
           (update acc :rejects conj
                   {:file label :line line
                    :reason (str "expected " (count header) " cells, got " (count cells))})))
       {:records [] :rejects []}
       (rest rows)))))

;; -----------------------------------------------------------------------
;; Field parsers. Each returns {:ok v} or {:error "why"}.

(def ^:private number-re #"[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?")

(defn- parse-slot [^String s interval-s]
  (let [odt (try (OffsetDateTime/parse (str/trim s))
                 (catch DateTimeParseException _ nil))]
    (if (nil? odt)
      {:error (str "unparseable timestamp '" s "' (need ISO 8601 with offset, like 2026-03-01T00:00:00Z)")}
      (let [inst (.toInstant odt)
            sec (.getEpochSecond inst)]
        (if (or (pos? (.getNano inst)) (not (zero? (mod sec interval-s))))
          {:error (str "timestamp '" s "' is not aligned to a " (quot interval-s 60) " minute interval in UTC")}
          {:ok (quot sec interval-s)})))))

(defn- parse-double-field [^String s field lo hi]
  (let [s (str/trim s)]
    (if-not (re-matches number-re s)
      {:error (str field " is not a plain decimal number: '" s "'")}
      (let [d (Double/parseDouble s)]
        (cond
          (or (Double/isNaN d) (Double/isInfinite d)) {:error (str field " is not finite")}
          (< d lo) {:error (str field " is below " lo ": " s)}
          (> d hi) {:error (str field " exceeds sanity cap " hi ": " s)}
          :else {:ok d})))))

(defn- parse-wh [s field]
  (let [r (parse-double-field s field 0.0 max-kwh-per-row)]
    (if (:ok r) {:ok (Math/round (* ^double (:ok r) 1000.0))} r)))

(defn- norm-region [s] (str/upper-case (str/trim s)))

;; -----------------------------------------------------------------------
;; Policy

(defn- norm-deliverability [d]
  (when-not (map? d) (fatal ":deliverability must be a map of load region to a set of supply regions"))
  (into {}
        (map (fn [[k v]]
               (when-not (and (string? k) (or (set? v) (sequential? v)) (every? string? v))
                 (fatal (str ":deliverability entry for " (pr-str k) " must map a string to a set of strings")))
               [(norm-region k) (set (map norm-region v))]))
        d))

(defn validate-policy
  "Merges a user policy over the defaults and rejects anything ambiguous."
  [user]
  (let [p (merge default-policy user)
        nn (fn [k] (let [v (get p k)]
                     (when-not (and (integer? v) (<= 0 v 8784))
                       (fatal (str k " must be an integer between 0 and 8784, got " (pr-str v))))
                     (long v)))]
    (when-not (contains? valid-intervals (:interval-minutes p))
      (fatal (str ":interval-minutes must be one of " (sort valid-intervals))))
    (when-not (#{:grid :fail} (:residual-fallback p))
      (fatal ":residual-fallback must be :grid or :fail"))
    (-> p
        (assoc :window-before (nn :window-before) :window-after (nn :window-after))
        (update :deliverability norm-deliverability))))

;; -----------------------------------------------------------------------
;; Ingest

(defn- reject [label line reason] {:file label :line line :reason reason})

(defn ingest-load
  "Returns {:loads {[region slot] wh} :rejects [...]}. Rows from different
  meters add up. The same meter, region and interval twice is rejected, as
  a duplicated export would otherwise double the load."
  [file policy]
  (let [isec (* 60 (:interval-minutes policy))
        {:keys [records rejects]} (read-table "load" file ["hour" "region" "kwh"])]
    (loop [rs records seen #{} loads {} rej rejects]
      (if-let [{:keys [line m]} (first rs)]
        (let [slot (parse-slot (m "hour") isec)
              wh (parse-wh (m "kwh") "kwh")
              region (norm-region (m "region"))
              meter (str/trim (get m "meter" ""))
              k [meter region (:ok slot)]]
          (cond
            (str/blank? region) (recur (rest rs) seen loads (conj rej (reject "load" line "region is blank")))
            (:error slot) (recur (rest rs) seen loads (conj rej (reject "load" line (:error slot))))
            (:error wh) (recur (rest rs) seen loads (conj rej (reject "load" line (:error wh))))
            (contains? seen k) (recur (rest rs) seen loads
                                      (conj rej (reject "load" line (str "duplicate load row for meter '" meter "', region " region " and that interval"))))
            :else (recur (rest rs) (conj seen k)
                         (update loads [region (:ok slot)] (fnil + 0) (:ok wh)) rej)))
        {:loads loads :rejects rej}))))

(defn ingest-supply
  "Returns {:certs [{:id :region :slot :wh :g :asset :line}] :rejects [...]}.
  A certificate id may appear once. The second sighting is rejected even if
  it is identical, because two rows with one id means the same MWh is being
  offered twice."
  [file policy]
  (let [isec (* 60 (:interval-minutes policy))
        {:keys [records rejects]} (read-table "supply" file ["certificate_id" "region" "hour" "kwh"])]
    (loop [rs records seen #{} certs [] rej rejects]
      (if-let [{:keys [line m]} (first rs)]
        (let [id (str/trim (m "certificate_id"))
              region (norm-region (m "region"))
              slot (parse-slot (m "hour") isec)
              wh (parse-wh (m "kwh") "kwh")
              g (if (str/blank? (get m "g_per_kwh" ""))
                  {:ok 0.0}
                  (parse-double-field (m "g_per_kwh") "g_per_kwh" 0.0 max-g-per-kwh))]
          (cond
            (str/blank? id) (recur (rest rs) seen certs (conj rej (reject "supply" line "certificate_id is blank")))
            (str/blank? region) (recur (rest rs) seen certs (conj rej (reject "supply" line "region is blank")))
            (:error slot) (recur (rest rs) seen certs (conj rej (reject "supply" line (:error slot))))
            (:error wh) (recur (rest rs) seen certs (conj rej (reject "supply" line (:error wh))))
            (:error g) (recur (rest rs) seen certs (conj rej (reject "supply" line (:error g))))
            (contains? seen id) (recur (rest rs) seen certs
                                       (conj rej (reject "supply" line (str "certificate_id '" id "' already used on an earlier row"))))
            :else (recur (rest rs) (conj seen id)
                         (conj certs {:id id :region region :slot (:ok slot) :wh (:ok wh)
                                      :g (:ok g) :asset (str/trim (get m "asset" "")) :line line})
                         rej)))
        {:certs certs :rejects rej}))))

(defn ingest-grid
  "Returns {:grid {[region slot] {:grid g :residual r-or-nil}} :rejects [...]}."
  [file policy]
  (let [isec (* 60 (:interval-minutes policy))
        {:keys [records rejects]} (read-table "grid" file ["hour" "region" "grid_g_per_kwh"])]
    (loop [rs records grid {} rej rejects]
      (if-let [{:keys [line m]} (first rs)]
        (let [region (norm-region (m "region"))
              slot (parse-slot (m "hour") isec)
              g (parse-double-field (m "grid_g_per_kwh") "grid_g_per_kwh" 0.0 max-g-per-kwh)
              rtxt (get m "residual_g_per_kwh" "")
              r (if (str/blank? rtxt) {:ok nil} (parse-double-field rtxt "residual_g_per_kwh" 0.0 max-g-per-kwh))
              k [region (:ok slot)]]
          (cond
            (str/blank? region) (recur (rest rs) grid (conj rej (reject "grid" line "region is blank")))
            (:error slot) (recur (rest rs) grid (conj rej (reject "grid" line (:error slot))))
            (:error g) (recur (rest rs) grid (conj rej (reject "grid" line (:error g))))
            (:error r) (recur (rest rs) grid (conj rej (reject "grid" line (:error r))))
            (contains? grid k) (recur (rest rs) grid (conj rej (reject "grid" line "duplicate factor row for region and interval")))
            :else (recur (rest rs) (assoc grid k {:grid (:ok g) :residual (:ok r)}) rej)))
        {:grid grid :rejects rej}))))

;; -----------------------------------------------------------------------
;; Dinic maximum flow on primitive arrays, iterative DFS so a long residual
;; path can never overflow the JVM stack.

(defn- dinic-phase!
  "Runs BFS plus blocking flow until no augmenting path remains. Mutates cap.
  Returns the total flow pushed in this call."
  [n ^ints head ^ints nxt ^ints to ^longs cap s t]
  (let [n (long n) s (long s) t (long t)
        level (int-array n)
        cursor (int-array n)
        queue (int-array n)
        path (int-array (inc n))
        bfs (fn []
              (java.util.Arrays/fill level -1)
              (aset level s 0)
              (aset queue 0 (int s))
              (loop [qh 0 qt 1]
                (if (< qh qt)
                  (let [u (aget queue qh)
                        qt' (loop [e (aget head u) qt qt]
                              (if (neg? e)
                                qt
                                (let [v (aget to e)]
                                  (if (and (pos? (aget cap e)) (neg? (aget level v)))
                                    (do (aset level v (inc (aget level u)))
                                        (aset queue qt v)
                                        (recur (aget nxt e) (inc qt)))
                                    (recur (aget nxt e) qt)))))]
                    (recur (inc qh) qt'))
                  (>= (aget level t) 0))))
        blocking (fn []
                   (System/arraycopy head 0 cursor 0 n)
                   (loop [u s d 0 total 0]
                     (if (== u t)
                       (let [b (loop [i 0 b Long/MAX_VALUE]
                                 (if (< i d) (recur (inc i) (min b (aget cap (aget path i)))) b))]
                         (dotimes [i d]
                           (let [e (aget path i)]
                             (aset cap e (- (aget cap e) b))
                             (aset cap (bit-xor e 1) (+ (aget cap (bit-xor e 1)) b))))
                         (let [k (loop [i 0] (if (zero? (aget cap (aget path i))) i (recur (inc i))))]
                           (recur (long (aget to (bit-xor (aget path k) 1))) k (+ total b))))
                       (let [lu (aget level u)
                             e (loop [e (aget cursor u)]
                                 (if (or (neg? e)
                                         (and (pos? (aget cap e)) (== (aget level (aget to e)) (inc lu))))
                                   e
                                   (recur (aget nxt e))))]
                         (aset cursor u (int e))
                         (if (neg? e)
                           (if (== u s)
                             total
                             (do (aset level u -1)
                                 (let [d' (dec d)
                                       pe (aget path d')]
                                   (recur (long (aget to (bit-xor pe 1))) d' total))))
                           (do (aset path d (int e))
                               (recur (long (aget to e)) (inc d) total)))))))]
    (loop [flow 0]
      (if (bfs) (recur (+ flow (long (blocking)))) flow))))

;; -----------------------------------------------------------------------
;; Matching

(defn- tier-of
  "0 same region and interval, 1 same region other interval, 2 other region
  same interval, 3 everything else. Lower tiers are activated first."
  [same-region? same-slot?]
  (cond (and same-region? same-slot?) 0
        same-region? 1
        same-slot? 2
        :else 3))

(defn- attribute-certs
  "Splits the Wh flowing out of one supply node across its certificates in
  id order. flows is a seq of {:load-key k :wh n}."
  [certs flows]
  (loop [cs (seq certs) left (some-> (first certs) :wh)
         fs (seq flows) need (some-> (first flows) :wh) out []]
    (if (or (nil? cs) (nil? fs))
      out
      (let [take (min left need)
            left' (- left take)
            need' (- need take)]
        (recur (if (zero? left') (next cs) cs)
               (if (zero? left') (some-> (second cs) :wh) left')
               (if (zero? need') (next fs) fs)
               (if (zero? need') (some-> (second fs) :wh) need')
               (conj out {:cert (first cs) :load-key (:load-key (first fs)) :wh take}))))))

(defn match-supply
  "Maximum matching of certificates to load under the policy. Returns
  {:allocations [{:cert c :load-key [region slot] :wh n}]
   :matched {load-key wh} :used {supply-key wh} :supply {supply-key wh}
   :total-wh n :edges n}."
  [loads certs policy]
  (let [{:keys [window-before window-after deliverability]} policy
        load-keys (vec (sort (keep (fn [[k wh]] (when (pos? wh) k)) loads)))
        groups (group-by (juxt :region :slot) (filter #(pos? (:wh %)) certs))
        supply-keys (vec (sort (keys groups)))
        supply-wh (into {} (map (fn [[k cs]] [k (reduce + (map :wh cs))])) groups)
        nl (count load-keys)
        ns (count supply-keys)
        load-node (zipmap load-keys (range 2 (+ 2 nl)))
        supply-node (zipmap supply-keys (range (+ 2 nl) (+ 2 nl ns)))
        load-regions (distinct (map first load-keys))
        accepts (reduce (fn [acc lr]
                          (reduce (fn [a sr] (update a sr (fnil conj []) lr))
                                  acc (conj (get deliverability lr #{}) lr)))
                        {} load-regions)
        edge-seq (fn []
                   (for [[sr ss :as sk] supply-keys
                         lr (get accepts sr)
                         t (range (- ss window-after) (+ ss window-before 1))
                         :let [lk [lr t]]
                         :when (contains? load-node lk)]
                     [(tier-of (= lr sr) (= t ss)) (Math/abs (long (- t ss))) sk lk]))
        edge-count (count (take (inc max-flow-edges) (edge-seq)))]
    (when (> edge-count max-flow-edges)
      (fatal (str "matching graph exceeds " max-flow-edges " edges; shrink the window or the period")))
    (if (or (zero? nl) (zero? ns))
      {:allocations [] :matched {} :used {} :supply supply-wh :total-wh 0 :edges 0}
      (let [edges (vec (sort (edge-seq)))
            ne (count edges)
            n (+ 2 nl ns)
            m (* 2 (+ nl ns ne))
            head (int-array n -1)
            nxt (int-array m -1)
            to (int-array m 0)
            cap (long-array m 0)
            ec (long-array 1)
            add-edge! (fn [u v c]
                        (let [e (aget ec 0)]
                          (aset to e (int v)) (aset cap e (long c))
                          (aset nxt e (aget head u)) (aset head u (int e))
                          (aset to (inc e) (int u)) (aset cap (inc e) 0)
                          (aset nxt (inc e) (aget head v)) (aset head v (int (inc e)))
                          (aset ec 0 (+ e 2))
                          e))]
        (doseq [sk supply-keys] (add-edge! 0 (supply-node sk) (supply-wh sk)))
        (doseq [lk load-keys] (add-edge! (load-node lk) 1 (loads lk)))
        (let [placed (mapv (fn [[tier _ sk lk]]
                             {:tier tier :sk sk :lk lk
                              :e (add-edge! (supply-node sk) (load-node lk) 0)})
                           edges)
              total (reduce
                     (fn [acc tier]
                       (doseq [{:keys [e]} (filter #(= tier (:tier %)) placed)]
                         (aset cap e inf-cap))
                       (+ acc (dinic-phase! n head nxt to cap 0 1)))
                     0 (range 4))
              flows (->> placed
                         (keep (fn [{:keys [sk lk e]}]
                                 (let [f (aget cap (bit-xor e 1))]
                                   (when (pos? f) {:supply-key sk :load-key lk :wh f})))))
              by-supply (group-by :supply-key flows)
              allocations (vec (mapcat (fn [sk]
                                         (attribute-certs (sort-by :id (groups sk))
                                                          (sort-by :load-key (get by-supply sk))))
                                       (filter by-supply supply-keys)))]
          {:allocations allocations
           :matched (reduce (fn [a {:keys [load-key wh]}] (update a load-key (fnil + 0) wh)) {} flows)
           :used (reduce (fn [a {:keys [supply-key wh]}] (update a supply-key (fnil + 0) wh)) {} flows)
           :supply supply-wh
           :total-wh total
           :edges ne})))))

;; -----------------------------------------------------------------------
;; Analysis

(defn- kwh ^double [wh] (/ (double wh) 1000.0))

(defn- ratio [a b] (when (pos? b) (/ (double a) (double b))))

(defn- slot-time [slot interval-s] (str (Instant/ofEpochSecond (* slot interval-s))))

(defn- load-gaps
  "Intervals missing between the first and last load row of each region.
  Silent gaps make a matching percentage look better than it is."
  [loads isec]
  (->> (group-by first (keys loads))
       (keep (fn [[region ks]]
               (let [slots (map second ks)
                     lo (apply min slots) hi (apply max slots)
                     missing (- (inc (- hi lo)) (count (set slots)))]
                 (when (pos? missing)
                   {:region region :missing_intervals missing
                    :first_missing (slot-time (first (remove (set slots) (range lo (inc hi)))) isec)}))))
       (sort-by :region)
       vec))

(defn analyze
  "Combines ingested inputs into the full report map."
  [{:keys [loads certs grid policy rejects]}]
  (let [isec (* 60 (:interval-minutes policy))
        spd (quot 1440 (:interval-minutes policy))
        mr (match-supply loads certs policy)
        matched (:matched mr)
        alloc-by-load (group-by :load-key (:allocations mr))
        fallback-wh (volatile! 0)
        missing-wh (volatile! 0)
        rows
        (vec
         (for [[[region slot :as lk] wh] (sort-by key loads)
               :when (pos? wh)
               :let [mwh (get matched lk 0)
                     unm (- wh mwh)
                     f (get grid lk)
                     res (cond
                           (some? (:residual f)) (:residual f)
                           (and f (= :grid (:residual-fallback policy)))
                           (do (vswap! fallback-wh + unm) (:grid f))
                           (and f (= :fail (:residual-fallback policy)))
                           (fatal (str "no residual factor for " region " at " (slot-time slot isec) " and :residual-fallback is :fail"))
                           :else nil)
                     _ (when (nil? f) (vswap! missing-wh + wh))
                     cert-g (reduce + 0.0 (map #(* (kwh (:wh %)) (:g (:cert %))) (alloc-by-load lk)))
                     market-g (when (or f (zero? unm))
                                (+ cert-g (if (pos? unm) (* (kwh unm) (or res 0.0)) 0.0)))]]
           {:region region :slot slot :time (slot-time slot isec)
            :load-wh wh :matched-wh mwh :unmatched-wh unm
            :grid-g (:grid f) :residual-g res
            :location-g (when f (* (kwh wh) (:grid f)))
            :market-g market-g
            :unmatched-g (when (and res (pos? unm)) (* (kwh unm) res))}))
        regions (sort (distinct (map :region rows)))
        supply-by-region (reduce (fn [a c] (update a (:region c) (fnil + 0) (:wh c))) {} certs)
        used-by-supply-region (reduce (fn [a [[r _] w]] (update a r (fnil + 0) w)) {} (:used mr))
        total-load (reduce + (map :load-wh rows))
        total-matched (reduce + (map :matched-wh rows))
        total-supply (reduce + (map :wh certs))
        avg-cert-g (fn [region]
                     (let [cs (filter #(= region (:region %)) certs)
                           w (reduce + (map :wh cs))]
                       (if (pos? w) (/ (reduce + (map #(* (double (:wh %)) (:g %)) cs)) w) 0.0)))
        region-rows
        (vec
         (for [r regions
               :let [rs (filter #(= r (:region %)) rows)
                     lw (reduce + (map :load-wh rs))
                     mw (reduce + (map :matched-wh rs))
                     own (get supply-by-region r 0)
                     annual (ratio (min lw own) lw)
                     loc (reduce + 0.0 (keep :location-g rs))
                     mkt (reduce + 0.0 (keep :market-g rs))
                     unhedged (reduce + 0.0 (map (fn [x] (* (kwh (:load-wh x)) (or (:residual-g x) (:grid-g x) 0.0))) rs))
                     annual-g (when annual (+ (* (- 1.0 annual) unhedged)
                                              (* annual (kwh lw) (avg-cert-g r))))]]
           (array-map
            :region r
            :load_kwh (kwh lw) :matched_kwh (kwh mw)
            :hourly_cfe (ratio mw lw)
            :annual_style_coverage annual
            :claim_gap_points (when annual (* 100.0 (- annual (ratio mw lw))))
            :own_region_supply_kwh (kwh own)
            :surplus_kwh (kwh (- own (get used-by-supply-region r 0)))
            :location_based_tco2 (/ loc 1.0e6)
            :hourly_market_based_tco2 (/ mkt 1.0e6)
            :annual_style_market_tco2 (when annual-g (/ annual-g 1.0e6)))))
        by-slot-of-day (->> rows
                            (group-by #(mod (:slot %) spd))
                            (sort-by key)
                            (mapv (fn [[sod rs]]
                                    (let [lw (reduce + (map :load-wh rs))
                                          mw (reduce + (map :matched-wh rs))
                                          minutes (* sod (:interval-minutes policy))]
                                      {:utc (fmt "%02d:%02d" (quot minutes 60) (mod minutes 60))
                                       :load_kwh (kwh lw) :matched_kwh (kwh mw)
                                       :cfe (ratio mw lw)}))))
        worst (->> rows (filter :unmatched-g) (sort-by (comp - :unmatched-g)) (take 10)
                   (mapv (fn [x] {:time (:time x) :region (:region x)
                                  :unmatched_kwh (kwh (:unmatched-wh x))
                                  :residual_g_per_kwh (:residual-g x)
                                  :unmatched_kg_co2 (/ (:unmatched-g x) 1000.0)})))
        reachable (set (mapcat (fn [lr] (conj (get (:deliverability policy) lr #{}) lr))
                               regions))
        unreachable-wh (reduce + (map :wh (remove #(contains? reachable (:region %)) certs)))
        total-surplus (- total-supply total-matched)
        warnings (cond-> []
                   (seq (load-gaps loads isec))
                   (conj {:code "load_gaps" :detail (load-gaps loads isec)})
                   (pos? @missing-wh)
                   (conj {:code "missing_grid_factor" :load_kwh (kwh @missing-wh)
                          :detail "load intervals without a grid factor row are left out of every emissions figure"})
                   (pos? @fallback-wh)
                   (conj {:code "residual_fallback" :unmatched_kwh (kwh @fallback-wh)
                          :detail "residual mix missing, grid average used for unmatched load"})
                   (pos? unreachable-wh)
                   (conj {:code "unreachable_supply" :kwh (kwh unreachable-wh)
                          :detail "certificates in regions that no load region accepts under the deliverability policy"})
                   (empty? loads)
                   (conj {:code "no_load" :detail "no valid load rows"}))
        annual-total (ratio (min total-load total-supply) total-load)]
    {:report
     (array-map
      :summary
      (array-map
       :interval_minutes (:interval-minutes policy)
       :window_before (:window-before policy)
       :window_after (:window-after policy)
       :load_kwh (kwh total-load)
       :certificate_kwh (kwh total-supply)
       :matched_kwh (kwh total-matched)
       :surplus_certificate_kwh (kwh total-surplus)
       :hourly_cfe (ratio total-matched total-load)
       :annual_style_coverage annual-total
       :claim_gap_points (when annual-total (* 100.0 (- annual-total (ratio total-matched total-load))))
       :location_based_tco2 (/ (reduce + 0.0 (keep :location-g rows)) 1.0e6)
       :hourly_market_based_tco2 (/ (reduce + 0.0 (keep :market-g rows)) 1.0e6)
       :graph_edges (:edges mr)
       :rejected_rows (count rejects))
      :regions region-rows
      :time_of_day_profile_utc by-slot-of-day
      :worst_unmatched_intervals worst
      :warnings warnings
      :rejects (vec (take 50 rejects))
      :rejects_truncated (> (count rejects) 50))
     :rows rows
     :allocations (:allocations mr)
     :isec isec}))

;; -----------------------------------------------------------------------
;; Output: JSON, text, CSV

(defn- json-escape ^String [^String s]
  (let [sb (StringBuilder. "\"")]
    (doseq [c s]
      (case c
        \" (.append sb "\\\"")
        \\ (.append sb "\\\\")
        \newline (.append sb "\\n")
        \return (.append sb "\\r")
        \tab (.append sb "\\t")
        (if (< (int c) 0x20) (.append sb (fmt "\\u%04x" (int c))) (.append sb c))))
    (str (.append sb "\""))))

(defn- json-num [x]
  (if (integer? x)
    (str x)
    (let [d (double x)]
      (if (or (Double/isNaN d) (Double/isInfinite d))
        "null"
        (let [bd (.setScale (BigDecimal. d) 6 java.math.RoundingMode/HALF_EVEN)]
          (if (zero? (.signum bd)) "0" (.toPlainString (.stripTrailingZeros bd))))))))

(defn to-json
  "Pretty printed JSON. Keyword keys have dashes turned into underscores."
  ([x] (to-json x 0))
  ([x depth]
   (let [pad (apply str (repeat (* 2 (inc depth)) \space))
         end (apply str (repeat (* 2 depth) \space))]
     (cond
       (nil? x) "null"
       (boolean? x) (str x)
       (number? x) (json-num x)
       (string? x) (json-escape x)
       (keyword? x) (json-escape (name x))
       (map? x) (if (empty? x) "{}"
                    (str "{\n"
                         (str/join ",\n" (map (fn [[k v]]
                                                (str pad (json-escape (str/replace (name k) "-" "_")) ": " (to-json v (inc depth))))
                                              x))
                         "\n" end "}"))
       (sequential? x) (if (empty? x) "[]"
                           (str "[\n" (str/join ",\n" (map #(str pad (to-json % (inc depth))) x)) "\n" end "]"))
       :else (json-escape (str x))))))

(defn- pct [x] (if x (fmt "%.2f%%" (* 100.0 x)) "n/a"))

(defn to-text [{:keys [report]}]
  (let [s (:summary report)
        lines (transient [])
        add! (fn [& xs] (conj! lines (apply str xs)))]
    (add! "Hourly energy matching ledger")
    (add! (fmt "  interval %d min, window before %d, window after %d"
               (:interval_minutes s) (:window_before s) (:window_after s)))
    (add! (fmt "  load %.3f kWh, certificates %.3f kWh, matched %.3f kWh, surplus %.3f kWh"
               (:load_kwh s) (:certificate_kwh s) (:matched_kwh s) (:surplus_certificate_kwh s)))
    (add! "  hourly CFE         " (pct (:hourly_cfe s)))
    (add! "  annual style claim " (pct (:annual_style_coverage s))
          (if (:claim_gap_points s) (fmt "   gap %.2f points" (:claim_gap_points s)) ""))
    (add! (fmt "  location based %.3f tCO2, hourly market based %.3f tCO2"
               (:location_based_tco2 s) (:hourly_market_based_tco2 s)))
    (add! "")
    (add! (fmt "%-10s %12s %12s %9s %9s" "region" "load kWh" "matched kWh" "CFE" "annual"))
    (doseq [r (:regions report)]
      (add! (fmt "%-10s %12.3f %12.3f %9s %9s" (:region r) (:load_kwh r) (:matched_kwh r)
                 (pct (:hourly_cfe r)) (pct (:annual_style_coverage r)))))
    (when (seq (:warnings report))
      (add! "")
      (doseq [w (:warnings report)]
        (add! "warning " (:code w) ": " (if (string? (:detail w)) (:detail w) (pr-str (:detail w))))))
    (when (pos? (:rejected_rows s))
      (add! "")
      (add! (:rejected_rows s) " input rows rejected")
      (doseq [r (take 10 (:rejects report))]
        (add! "  " (:file r) " line " (:line r) ": " (:reason r))))
    (str (str/join "\n" (persistent! lines)) "\n")))

(defn- csv-cell
  "Quotes when needed and defuses spreadsheet formula injection: ids and
  asset names come from untrusted files and may start with = + - or @."
  [x]
  (let [s (str x)
        s (if (and (seq s) (re-find #"^[=+@\t\r]" s)) (str "'" s) s)
        s (if (and (seq s) (re-find #"^-" s) (not (re-matches number-re s))) (str "'" s) s)]
    (if (re-find #"[\",\r\n]" s)
      (str "\"" (str/replace s "\"" "\"\"") "\"")
      s)))

(defn- csv-line [cells] (str (str/join "," (map csv-cell cells)) "\n"))

(defn hourly-csv [{:keys [rows]}]
  (apply str
         (csv-line ["interval_utc" "region" "load_kwh" "matched_kwh" "cfe"
                    "grid_g_per_kwh" "residual_g_per_kwh" "market_g_per_kwh"])
         (for [{:keys [time region load-wh matched-wh grid-g residual-g market-g]} rows]
           (csv-line [time region (fmt "%.3f" (kwh load-wh)) (fmt "%.3f" (kwh matched-wh))
                      (if-let [c (ratio matched-wh load-wh)] (fmt "%.6f" c) "")
                      (if grid-g (fmt "%.3f" grid-g) "")
                      (if residual-g (fmt "%.3f" residual-g) "")
                      (if (and market-g (pos? load-wh)) (fmt "%.3f" (/ market-g (kwh load-wh))) "")]))))

(defn alloc-csv [{:keys [allocations isec]}]
  (apply str
         (csv-line ["certificate_id" "asset" "supply_region" "supply_utc" "load_region" "load_utc"
                    "kwh" "lifecycle_g_per_kwh"])
         (for [{:keys [cert load-key wh]} allocations]
           (csv-line [(:id cert) (:asset cert) (:region cert) (slot-time (:slot cert) isec)
                      (first load-key) (slot-time (second load-key) isec)
                      (fmt "%.3f" (kwh wh)) (fmt "%.3f" (:g cert))]))))

(defn- write-atomically! [path content inputs]
  (let [target (.toAbsolutePath (Paths/get path (make-array String 0)))
        canon (fn [p] (try (.getCanonicalPath (io/file (str p))) (catch Exception _ (str p))))]
    (when (some #(= (canon target) (canon %)) inputs)
      (fatal (str "refusing to overwrite an input file: " path)))
    (let [dir (or (.getParent target) (Paths/get "." (make-array String 0)))
          tmp (Files/createTempFile dir ".ledger-" ".tmp" (make-array java.nio.file.attribute.FileAttribute 0))]
      (try
        (Files/write tmp (.getBytes ^String content "UTF-8") (make-array java.nio.file.OpenOption 0))
        (Files/move tmp target (into-array java.nio.file.CopyOption
                                          [StandardCopyOption/REPLACE_EXISTING StandardCopyOption/ATOMIC_MOVE]))
        (catch Exception e
          (Files/deleteIfExists tmp)
          (fatal (str "cannot write " path ": " (.getMessage e))))))))

;; -----------------------------------------------------------------------
;; CLI

(defn parse-args
  "Turns argv into an options map. Unknown flags are errors, never ignored."
  [args]
  (let [flags #{"--load" "--supply" "--grid" "--policy" "--interval-minutes" "--window-before"
                "--window-after" "--min-cfe" "--max-rejects" "--format" "--hourly-out" "--alloc-out"}]
    (loop [as args opts {}]
      (if-let [a (first as)]
        (cond
          (= a "--help") (recur (rest as) (assoc opts :help true))
          (contains? flags a)
          (if-let [v (second as)]
            (recur (drop 2 as) (assoc opts (keyword (subs a 2)) v))
            (fatal (str a " needs a value")))
          :else (fatal (str "unknown argument: " a)))
        opts))))

(defn- int-opt [opts k]
  (when-let [v (get opts k)]
    (try (Long/parseLong v)
         (catch NumberFormatException _ (fatal (str "--" (name k) " needs an integer, got '" v "'"))))))

(defn- build-policy [opts]
  (let [from-file (when-let [f (:policy opts)]
                    (let [v (try (edn/read-string {:readers {}} (slurp f))
                                 (catch Exception e (fatal (str "cannot read policy " f ": " (.getMessage e)))))]
                      (when-not (map? v) (fatal "policy file must hold one EDN map"))
                      v))
        cli (cond-> {}
              (:interval-minutes opts) (assoc :interval-minutes (int-opt opts :interval-minutes))
              (:window-before opts) (assoc :window-before (int-opt opts :window-before))
              (:window-after opts) (assoc :window-after (int-opt opts :window-after)))]
    (validate-policy (merge from-file cli))))

(defn run
  "Runs the whole pipeline for argv. Returns {:exit n :out string :err string}
  without touching System/exit so it can be driven from tests."
  [args]
  (try
    (let [opts (parse-args args)]
      (if (:help opts)
        {:exit 0 :out (str usage-text "\n") :err ""}
        (do
          (doseq [k [:load :supply :grid]]
            (when-not (get opts k) (fatal (str "--" (name k) " is required"))))
          (let [fmt-kind (get opts :format "json")
                _ (when-not (#{"json" "text"} fmt-kind) (fatal "--format must be json or text"))
                min-cfe (when-let [v (:min-cfe opts)]
                          (let [d (try (Double/parseDouble v) (catch NumberFormatException _ (fatal "--min-cfe needs a number")))]
                            (when-not (<= 0.0 d 1.0) (fatal "--min-cfe must be between 0 and 1"))
                            d))
                max-rejects (or (int-opt opts :max-rejects) 0)
                policy (build-policy opts)
                l (ingest-load (:load opts) policy)
                s (ingest-supply (:supply opts) policy)
                g (ingest-grid (:grid opts) policy)
                rejects (vec (concat (:rejects l) (:rejects s) (:rejects g)))
                result (analyze {:loads (:loads l) :certs (:certs s) :grid (:grid g)
                                 :policy policy :rejects rejects})
                inputs [(:load opts) (:supply opts) (:grid opts) (:policy opts)]]
            (when-let [p (:hourly-out opts)] (write-atomically! p (hourly-csv result) (remove nil? inputs)))
            (when-let [p (:alloc-out opts)] (write-atomically! p (alloc-csv result) (remove nil? inputs)))
            (let [cfe (get-in result [:report :summary :hourly_cfe])
                  exit (cond (> (count rejects) max-rejects) 3
                             (and min-cfe (or (nil? cfe) (< cfe min-cfe))) 1
                             :else 0)]
              {:exit exit
               :out (if (= fmt-kind "json") (str (to-json (:report result)) "\n") (to-text result))
               :err (case exit
                      3 (str (count rejects) " rejected input rows exceed --max-rejects " max-rejects "\n")
                      1 (str "hourly CFE " (pct cfe) " is below --min-cfe " (pct min-cfe) "\n")
                      "")})))))
    (catch clojure.lang.ExceptionInfo e
      (if (:fatal (ex-data e))
        {:exit 2 :out "" :err (str "error: " (.getMessage e) "\n\n" usage-text "\n")}
        (throw e)))))

(defn -main [& args]
  (let [{:keys [exit out err]} (run args)]
    (print out) (flush)
    (binding [*out* *err*] (print err) (flush))
    (System/exit exit)))

(when (some-> (System/getProperty "sun.java.command") (str/includes? "HourlyEnergyMatchingLedger.clj"))
  (apply -main *command-line-args*))
