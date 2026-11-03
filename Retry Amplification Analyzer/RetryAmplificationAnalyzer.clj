(ns retry-amplification-analyzer
  "Static analysis of retry policy across a service call graph.

  Input is an EDN topology. Output is a report of worst case load per service,
  deadline inversions, nested retries and policy smells, plus an exit code a CI
  job can gate on. Pure Clojure core, no dependencies."
  (:require [clojure.edn :as edn]
            [clojure.string :as str]))

(def severity-rank {:info 0 :low 1 :medium 2 :high 3 :critical 4})

(def default-options
  {:warn-amp 4.0
   :fail-amp 10.0
   :max-steps 200000
   :top-paths 3})

(def ^:private max-double Double/MAX_VALUE)

;; ---------------------------------------------------------------------------
;; Validation
;; ---------------------------------------------------------------------------

(defn- num-where [x pred] (and (number? x) (pred x)))

(defn validate
  "Returns a vector of human readable problems. Empty means the topology is usable."
  [{:keys [entry entry-rps services] :as topology}]
  (let [errs (volatile! [])
        err! (fn [& parts] (vswap! errs conj (apply str parts)))]
    (when-not (map? topology) (err! "topology must be a map"))
    (when-not (map? services) (err! ":services must be a map of service id to definition"))
    (when (and (map? services) (not (contains? services entry)))
      (err! ":entry " (pr-str entry) " is not a key of :services"))
    (when-not (num-where entry-rps pos?) (err! ":entry-rps must be a positive number"))
    (when (map? services)
      (doseq [[svc def] services]
        (let [{:keys [capacity-rps mode local-ms calls]} def]
          (when-not (map? def) (err! svc ": definition must be a map"))
          (when (and (some? capacity-rps) (not (num-where capacity-rps pos?)))
            (err! svc ": :capacity-rps must be positive"))
          (when-not (contains? #{nil :sequential :parallel} mode)
            (err! svc ": :mode must be :sequential or :parallel"))
          (when (and (some? local-ms) (not (num-where local-ms #(>= % 0))))
            (err! svc ": :local-ms must be zero or more"))
          (when-not (or (nil? calls) (sequential? calls))
            (err! svc ": :calls must be a vector"))
          (doseq [[i c] (map-indexed vector (when (sequential? calls) calls))]
            (let [where (str svc " call #" i)
                  {:keys [to fanout retries timeout-ms backoff-ms backoff-cap-ms budget-ratio]} c]
              (cond
                (not (map? c)) (err! where ": must be a map")
                :else
                (do
                  (when-not (contains? services to) (err! where ": unknown callee " (pr-str to)))
                  (when (and (some? fanout) (not (num-where fanout pos?)))
                    (err! where ": :fanout must be positive"))
                  (when (and (some? retries) (not (and (integer? retries) (>= retries 0))))
                    (err! where ": :retries must be a non negative integer"))
                  (when-not (num-where timeout-ms pos?)
                    (err! where ": :timeout-ms is required and must be positive"))
                  (when (and (some? backoff-ms) (not (num-where backoff-ms #(>= % 0))))
                    (err! where ": :backoff-ms must be zero or more"))
                  (when (and (some? backoff-cap-ms) (not (num-where backoff-cap-ms pos?)))
                    (err! where ": :backoff-cap-ms must be positive"))
                  (when (and (some? budget-ratio) (not (num-where budget-ratio #(and (>= % 0) (<= % 1)))))
                    (err! where ": :budget-ratio must be between 0 and 1")))))))))
    @errs))

;; ---------------------------------------------------------------------------
;; Normalisation and graph walk
;; ---------------------------------------------------------------------------

(defn- normalise-edge [from idx c]
  (-> c
      (assoc :from from :idx idx)
      (update :fanout #(double (or % 1)))
      (update :retries #(long (or % 0)))
      (update :backoff-ms #(double (or % 0)))
      (update :backoff-cap-ms #(double (or % max-double)))
      (update :timeout-ms double)))

(defn edges-of [{:keys [services]}]
  (vec (for [[from svc] services
             [i c] (map-indexed vector (:calls svc))]
         (normalise-edge from i c))))

(defn- adjacency [edges] (group-by :from edges))

(defn- walk
  "Depth first from entry. Returns post order (callees before callers) and any cycles."
  [adj entry]
  (let [state (volatile! {})
        post (volatile! [])
        cycles (volatile! #{})]
    (letfn [(visit [n path]
              (case (get @state n)
                :done nil
                :active (vswap! cycles conj (conj (vec (drop-while #(not= % n) path)) n))
                (do (vswap! state assoc n :active)
                    (doseq [e (get adj n)] (visit (:to e) (conj path n)))
                    (vswap! state assoc n :done)
                    (vswap! post conj n))))]
      (visit entry []))
    {:post @post :cycles (vec @cycles)}))

;; ---------------------------------------------------------------------------
;; Arithmetic
;; ---------------------------------------------------------------------------

(defn normal-factor [e] (:fanout e))

(defn storm-factor
  "Every attempt of every retry is spent: fanout * (1 + retries)."
  [e]
  (* (:fanout e) (inc (:retries e))))

(defn budgeted-factor
  "A retry budget caps extra attempts at budget-ratio of first attempts."
  [e]
  (let [extra (if-some [r (:budget-ratio e)] (min (double (:retries e)) (double r)) (double (:retries e)))]
    (* (:fanout e) (+ 1.0 extra))))

(defn backoff-total-ms
  "Sum of exponential backoff sleeps across all retries, with the cap applied per sleep."
  [{:keys [retries backoff-ms backoff-cap-ms]}]
  (reduce + 0.0
          (for [i (range retries)]
            (min backoff-cap-ms (* backoff-ms (Math/pow 2.0 i))))))

(defn edge-worst-ms
  "Wall clock if every attempt runs to its timeout."
  [e]
  (+ (* (inc (:retries e)) (:timeout-ms e)) (backoff-total-ms e)))

(defn multipliers
  "Requests reaching each service per one entry request, under the given edge factor."
  [order adj entry factor]
  (reduce (fn [acc n]
            (let [m (get acc n 0.0)]
              (reduce (fn [a e] (update a (:to e) (fnil + 0.0) (* m (factor e))))
                      acc (get adj n))))
          {entry 1.0}
          order))

(defn- node-worst-times [post adj services]
  (reduce (fn [acc n]
            (let [svc (get services n)
                  seq? (not= :parallel (:mode svc))
                  durs (for [e (get adj n)]
                         (* (if seq? (:fanout e) 1.0) (edge-worst-ms e)))
                  combined (if seq? (reduce + 0.0 durs) (reduce max 0.0 durs))]
              (assoc acc n (+ (double (or (:local-ms svc) 0)) combined))))
          {}
          post))

(defn- retry-depths [post adj]
  (reduce (fn [acc n]
            (assoc acc n
                   (reduce max 0
                           (for [e (get adj n)]
                             (+ (if (pos? (:retries e)) 1 0) (get acc (:to e) 0))))))
          {}
          post))

(defn- ancestors-of [edges target]
  (let [rev (group-by :to edges)]
    (loop [seen #{target} frontier [target]]
      (if-let [n (first frontier)]
        (let [new (remove seen (map :from (get rev n)))]
          (recur (into seen new) (into (subvec frontier 1) new)))
        seen))))

(defn heaviest-paths
  "Entry to target paths ranked by storm multiplier. Bounded by :max-steps."
  [edges adj entry target {:keys [max-steps top-paths]}]
  (let [can (ancestors-of edges target)
        steps (volatile! 0)
        found (volatile! [])
        truncated (volatile! false)]
    (letfn [(go [n path mult]
              (cond
                (> (vswap! steps inc) max-steps) (vreset! truncated true)
                (= n target) (vswap! found conj {:path path :multiplier mult})
                :else (doseq [e (get adj n)
                              :when (contains? can (:to e))]
                        (go (:to e) (conj path (:to e)) (* mult (storm-factor e))))))]
      (when (contains? can entry) (go entry [entry] 1.0)))
    {:paths (vec (take top-paths (sort-by (comp - :multiplier) @found)))
     :truncated? @truncated}))

;; ---------------------------------------------------------------------------
;; Findings
;; ---------------------------------------------------------------------------

(defn- finding [id severity where message fix]
  {:id id :severity severity :where where :message message :fix fix})

(defn- edge-label [e] (str (name (:from e)) " -> " (name (:to e))))

(defn- fmt [x] (if (>= x 100) (format "%.0f" (double x)) (format "%.2f" (double x))))

(defn- policy-findings [edges depths]
  (mapcat
   (fn [e]
     (let [lbl (edge-label e)
           retrying? (pos? (:retries e))]
       (concat
        (when (and retrying? (false? (:idempotent e)))
          [(finding :non-idempotent-retry :high lbl
                    (str "Retries " (:retries e) " times on a call declared non idempotent.")
                    "Remove the retry or add an idempotency key the callee honours, then mark it idempotent.")])
        (when (and retrying? (nil? (:idempotent e)))
          [(finding :idempotency-undeclared :medium lbl
                    "Retries are enabled but :idempotent is not declared."
                    "Declare :idempotent true or false so the analyzer and reviewers know the retry is safe.")])
        (when (and retrying? (not (true? (:jitter e))))
          [(finding :no-jitter :medium lbl
                    "Retries without jitter put every client on the same schedule and create synchronised waves."
                    "Use full or decorrelated jitter on the backoff.")])
        (when (and retrying? (nil? (:budget-ratio e)))
          [(finding :no-retry-budget :medium lbl
                    "No retry budget. Under a full outage this edge multiplies load by its full attempt count."
                    "Add a token bucket or ratio budget (for example 10 percent of first attempts) and declare :budget-ratio.")])
        (when (and retrying? (pos? (get depths (:to e) 0)))
          [(finding :nested-retry :medium lbl
                    "This edge retries and so does something below it. Attempts multiply instead of add."
                    "Retry at one layer only, ideally the one closest to the failing dependency, and fail fast above it.")])
        (when (and retrying? (zero? (:backoff-ms e)))
          [(finding :zero-backoff :low lbl
                    "Retries fire immediately after a failure."
                    "Set :backoff-ms so a struggling callee gets breathing room.")]))))
   edges))

(defn- deadline-findings [edges worst]
  (for [e edges
        :let [inner (get worst (:to e) 0.0)]
        :when (> inner (:timeout-ms e))]
    (finding :deadline-inversion :high (edge-label e)
             (str "Caller gives up after " (fmt (:timeout-ms e)) " ms per attempt but "
                  (name (:to e)) " can keep working for " (fmt inner)
                  " ms. The extra time is orphaned work nobody is waiting for.")
             "Shrink the inner timeouts and retries or pass a propagated deadline so the callee stops when the caller does.")))

(defn- load-findings [reachable services normal storm budgeted edges adj entry opts]
  (mapcat
   (fn [n]
     (let [cap (:capacity-rps (get services n))
           nl (get normal n 0.0) sl (get storm n 0.0) bl (get budgeted n 0.0)
           amp (when (pos? nl) (/ sl nl))
           paths (delay (heaviest-paths edges adj entry n opts))]
       (concat
        (when (and cap (> nl cap))
          [(finding :already-overloaded :critical (name n)
                    (str "Steady state load " (fmt nl) " rps already exceeds capacity " (fmt cap) " rps.")
                    "Raise capacity or cut fanout before looking at retries.")])
        (when (and cap (<= nl cap) (> bl cap))
          [(finding :retry-storm-overload :critical (name n)
                    (str "Healthy load " (fmt nl) " rps. Worst case with declared budgets " (fmt bl)
                         " rps against capacity " (fmt cap) " rps. A failure below this service tips it over.")
                    (str "Cut retries on the heaviest path or add budgets. Heaviest paths: "
                         (str/join "; "
                                   (for [{:keys [path multiplier]} (:paths @paths)]
                                     (str (str/join " > " (map name path)) " x" (fmt multiplier))))
                         (when (:truncated? @paths) " (search truncated)")))])
        (when (and cap (<= bl cap) (> sl cap))
          [(finding :depends-on-retry-budget :medium (name n)
                    (str "Full outage load " (fmt sl) " rps exceeds capacity " (fmt cap)
                         " rps. Only the retry budgets keep it under (" (fmt bl) " rps).")
                    "Make sure the budgets are enforced in code, not just declared, and alert when they run dry.")])
        (when (and amp (>= amp (:fail-amp opts)))
          [(finding :amplification-high :high (name n)
                    (str "Worst case amplification x" (fmt amp) " over healthy load.")
                    "Remove retries from upper layers or cap attempts per request across the whole call chain.")])
        (when (and amp (>= amp (:warn-amp opts)) (< amp (:fail-amp opts)))
          [(finding :amplification-warn :medium (name n)
                    (str "Worst case amplification x" (fmt amp) " over healthy load.")
                    "Review whether each layer needs its own retries.")]))))
   reachable))

;; ---------------------------------------------------------------------------
;; Analysis
;; ---------------------------------------------------------------------------

(defn analyze
  "Returns a report map, or {:errors [...]} when the topology is invalid."
  ([topology] (analyze topology {}))
  ([topology options]
   (let [errors (validate topology)]
     (if (seq errors)
       {:errors errors}
       (let [opts (merge default-options options)
             {:keys [entry entry-rps services]} topology
             edges (edges-of topology)
             adj (adjacency edges)
             {:keys [post cycles]} (walk adj entry)
             unreachable (vec (sort (remove (set post) (keys services))))
             unreach-f (when (seq unreachable)
                         [(finding :unreachable :info (str/join ", " (map name unreachable))
                                   "Defined but never called from the entry service." "Remove or wire them in.")])]
         (if (seq cycles)
           {:entry entry :entry-rps entry-rps :cycles cycles :nodes []
            :findings (into [(finding :cycle :critical
                                      (str/join " | " (map #(str/join " > " (map name %)) cycles))
                                      "Call cycle found. With retries on any edge a cycle has no finite worst case."
                                      "Break the cycle or make the back edge asynchronous.")]
                            unreach-f)
            :max-severity :critical}
           (let [order (reverse post)
                 reachable (vec order)
                 reach-edges (filterv #(contains? (set post) (:from %)) edges)
                 m-normal (multipliers order adj entry normal-factor)
                 m-storm (multipliers order adj entry storm-factor)
                 m-budget (multipliers order adj entry budgeted-factor)
                 scale (fn [m] (into {} (map (fn [[k v]] [k (* v entry-rps)]) m)))
                 normal (scale m-normal) storm (scale m-storm) budgeted (scale m-budget)
                 worst (node-worst-times post adj services)
                 depths (retry-depths post adj)
                 findings (->> (concat (policy-findings reach-edges depths)
                                       (deadline-findings reach-edges worst)
                                       (load-findings reachable services normal storm budgeted reach-edges adj entry opts)
                                       unreach-f)
                               (sort-by (juxt (comp - severity-rank :severity) :id :where))
                               vec)
                 nodes (vec (for [n reachable
                                  :let [cap (:capacity-rps (get services n))
                                        nl (get normal n 0.0)]]
                              {:service n
                               :normal-rps nl
                               :storm-rps (get storm n 0.0)
                               :budgeted-rps (get budgeted n 0.0)
                               :amplification (when (pos? nl) (/ (get storm n 0.0) nl))
                               :capacity-rps cap
                               :worst-ms (get worst n 0.0)}))]
             {:entry entry :entry-rps entry-rps :cycles [] :nodes nodes :findings findings
              :max-severity (if (seq findings)
                              (apply max-key severity-rank (map :severity findings))
                              :none)})))))))

;; ---------------------------------------------------------------------------
;; Rendering
;; ---------------------------------------------------------------------------

(defn render-text [{:keys [errors nodes findings entry entry-rps]}]
  (if (seq errors)
    (str "Invalid topology:\n" (str/join "\n" (map #(str "  - " %) errors)) "\n")
    (str
     "Entry " (name entry) " at " (fmt entry-rps) " rps\n\n"
     (format "%-16s %10s %10s %10s %7s %10s %10s%n" "service" "healthy" "storm" "budgeted" "amp" "capacity" "worst ms")
     (str/join
      (for [n nodes]
        (format "%-16s %10s %10s %10s %7s %10s %10s%n"
                (name (:service n)) (fmt (:normal-rps n)) (fmt (:storm-rps n)) (fmt (:budgeted-rps n))
                (if (:amplification n) (str "x" (fmt (:amplification n))) "-")
                (if (:capacity-rps n) (fmt (:capacity-rps n)) "-")
                (fmt (:worst-ms n)))))
     "\n"
     (if (empty? findings)
       "No findings.\n"
       (str/join
        "\n"
        (for [f findings]
          (str "[" (str/upper-case (name (:severity f))) "] " (name (:id f)) " @ " (:where f) "\n"
               "  " (:message f) "\n"
               "  Fix: " (:fix f) "\n")))))))

;; ---------------------------------------------------------------------------
;; CLI
;; ---------------------------------------------------------------------------

(def ^:private usage
  (str "Usage: clojure -M RetryAmplificationAnalyzer.clj TOPOLOGY.edn [--fail-on SEVERITY] "
       "[--format text|edn] [--warn-amp N] [--fail-amp N]\n"
       "Exit codes: 0 clean, 1 findings at or above --fail-on (default high), 2 bad input\n"))

(defn- parse-args [args]
  (loop [[a & more :as xs] args acc {:format "text" :fail-on :high}]
    (cond
      (empty? xs) acc
      (= a "--fail-on") (recur (rest more) (assoc acc :fail-on (keyword (first more))))
      (= a "--format") (recur (rest more) (assoc acc :format (first more)))
      (= a "--warn-amp") (recur (rest more) (assoc acc :warn-amp (Double/parseDouble (first more))))
      (= a "--fail-amp") (recur (rest more) (assoc acc :fail-amp (Double/parseDouble (first more))))
      (str/starts-with? a "--") (throw (ex-info (str "unknown flag " a) {}))
      (:file acc) (throw (ex-info "only one topology file is accepted" {}))
      :else (recur more (assoc acc :file a)))))

(defn run
  "Runs the CLI and returns an exit code. Prints to stdout and stderr."
  [args]
  (try
    (let [{:keys [file fail-on format] :as parsed} (parse-args args)]
      (when-not file (throw (ex-info "missing topology file" {})))
      (when-not (contains? severity-rank fail-on) (throw (ex-info (str "unknown severity " (name fail-on)) {})))
      (when-not (#{"text" "edn"} format) (throw (ex-info "format must be text or edn" {})))
      (let [topology (edn/read-string (slurp file))
            report (analyze topology (select-keys parsed [:warn-amp :fail-amp]))]
        (if (:errors report)
          (do (binding [*out* *err*] (print (render-text report)) (flush)) 2)
          (do (print (if (= format "edn") (str (pr-str report) "\n") (render-text report)))
              (flush)
              (if (>= (get severity-rank (:max-severity report) -1) (severity-rank fail-on)) 1 0)))))
    (catch Exception e
      (binding [*out* *err*] (println (str "error: " (.getMessage e))) (print usage) (flush))
      2)))

(when (seq *command-line-args*)
  (System/exit (run *command-line-args*)))
