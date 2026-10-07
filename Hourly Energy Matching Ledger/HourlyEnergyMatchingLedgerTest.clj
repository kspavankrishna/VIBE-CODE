(ns hourly-energy-matching-ledger-test
  "Run with: clojure -M HourlyEnergyMatchingLedgerTest.clj
  Checks the Dinic matcher against an independent Edmonds Karp oracle on
  random instances, then the invariants a ledger must never break."
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is run-tests testing successful?]]))

(load-file (str (.getParent (java.io.File. ^String *file*)) "/HourlyEnergyMatchingLedger.clj"))
(require '[hourly-energy-matching-ledger :as hl])

(defn oracle-max-flow
  "Plain map based Edmonds Karp over a residual capacity map. Slow and
  obviously correct, which is the point."
  [loads supply edges]
  (let [big Long/MAX_VALUE
        init (merge-with + 
                         (into {} (map (fn [[k w]] [[:src [:s k]] w]) supply))
                         (into {} (map (fn [[k w]] [[[:l k] :t] w]) loads))
                         (into {} (map (fn [[sk lk]] [[[:s sk] [:l lk]] big]) edges)))
        adj (reduce (fn [a [u v]] (-> a (update u (fnil conj #{}) v) (update v (fnil conj #{}) u))) {} (keys init))]
    (loop [res init total 0]
      (let [prev (loop [q [:src] seen {:src nil}]
                   (if (or (empty? q) (contains? seen :t))
                     seen
                     (let [u (first q)
                           nxt (filter #(and (not (contains? seen %)) (pos? (get res [u %] 0))) (get adj u))]
                       (recur (into (vec (rest q)) nxt) (reduce #(assoc %1 %2 u) seen nxt)))))]
        (if-not (contains? prev :t)
          total
          (let [path (loop [v :t acc []] (if (= v :src) acc (recur (get prev v) (conj acc [(get prev v) v]))))
                b (reduce min (map #(get res %) path))
                res' (reduce (fn [r [u v]] (-> r (update [u v] - b) (update [v u] (fnil + 0) b))) res path)]
            (recur res' (+ total b))))))))

(defn rand-nth' [^java.util.Random rnd coll] (nth coll (.nextInt rnd (count coll))))

(defn rand-instance [^java.util.Random rnd]
  (let [regions ["A" "B" "C"]
        slots (range 0 (+ 2 (.nextInt rnd 5)))
        loads (into {} (for [r regions s slots :when (< (.nextInt rnd 10) 7)] [[r s] (long (inc (.nextInt rnd 50)))]))
        certs (vec (for [i (range (+ 3 (.nextInt rnd 12)))]
                     {:id (str "C" (format "%03d" i)) :region (rand-nth' rnd regions) :slot (.nextInt rnd 6)
                      :wh (long (inc (.nextInt rnd 60))) :g 0.0}))
        deliv (into {} (for [r regions :when (.nextBoolean rnd)] [r (set (filter #(not= r %) (take (.nextInt rnd 3) (shuffle regions))))]))]
    {:loads loads :certs certs
     :policy (hl/validate-policy {:window-before (.nextInt rnd 3) :window-after (.nextInt rnd 3)
                                  :deliverability (into {} (map (fn [[k v]] [k v]) deliv))})}))

(defn naive-edges [{:keys [loads certs policy]}]
  (let [{:keys [window-before window-after deliverability]} policy]
    (distinct
     (for [c certs [[lr t] w] loads :when (pos? w)
           :when (and (or (= lr (:region c)) (contains? (get deliverability lr #{}) (:region c)))
                      (<= (- t window-before) (:slot c) (+ t window-after)))]
       [[(:region c) (:slot c)] [lr t]]))))

(deftest matches-oracle-and-keeps-invariants
  (let [rnd (java.util.Random. 20260607)]
    (dotimes [i 150]
      (let [{:keys [loads certs policy] :as inst} (rand-instance rnd)
            mr (hl/match-supply loads certs policy)
            supply (reduce (fn [a c] (update a [(:region c) (:slot c)] (fnil + 0) (:wh c))) {} certs)
            oracle (oracle-max-flow loads supply (naive-edges inst))]
        (testing (str "instance " i)
          (is (= oracle (:total-wh mr)) "total matched equals the oracle optimum")
          (is (= (:total-wh mr) (reduce + (map :wh (:allocations mr)))) "allocations sum to total")
          (doseq [[k w] (:matched mr)] (is (<= w (get loads k 0)) "never over cover a load"))
          (let [per-cert (reduce (fn [a {:keys [cert wh]}] (update a (:id cert) (fnil + 0) wh)) {} (:allocations mr))]
            (doseq [c certs] (is (<= (get per-cert (:id c) 0) (:wh c)) "never over spend a certificate")))
          (doseq [{:keys [cert load-key]} (:allocations mr)]
            (let [[lr t] load-key]
              (is (or (= lr (:region cert)) (contains? (get (:deliverability policy) lr #{}) (:region cert))))
              (is (<= (- t (:window-before policy)) (:slot cert) (+ t (:window-after policy)))))))))))

(deftest csv-parser-edge-cases
  (is (= [["a" "b,c" "d\"e"]] (map :cells (hl/parse-csv "a,\"b,c\",\"d\"\"e\""))))
  (is (= [["h1" "h2"] ["x" "y"]] (map :cells (hl/parse-csv "﻿h1,h2\r\nx,y\r\n"))))
  (is (= [["a" "line1\nline2"] ["z" ""]] (map :cells (hl/parse-csv "a,\"line1\nline2\"\nz,"))))
  (is (= [2 4] (map :line (drop 1 (hl/parse-csv "a\n\"x\ny\"\nb")))) "line numbers survive quoted newlines")
  (is (thrown? clojure.lang.ExceptionInfo (hl/parse-csv "a,\"never closed"))))

(defn tmp-file [content]
  (let [f (java.io.File/createTempFile "ledger" ".csv")]
    (.deleteOnExit f) (spit f content) (.getPath f)))

(deftest ingest-rejects-bad-rows-without-hiding-them
  (let [p (hl/validate-policy {})
        l (hl/ingest-load (tmp-file (str "hour,region,kwh,meter\n"
                                         "2026-06-01T00:00:00Z,de,10,m1\n"
                                         "2026-06-01T00:00:00Z,DE,10,m1\n"
                                         "2026-06-01T00:00:00Z,DE,5,m2\n"
                                         "2026-06-01T00:30:00Z,DE,5,m2\n"
                                         "2026-06-01T01:00:00Z,DE,-4,m2\n"
                                         "2026-06-01T02:00:00Z,DE,1d,m2\n"
                                         "2026-06-01T03:00:00+02:00,DE,3,m2\n"
                                         "nonsense,DE,3,m2\n")) p)]
    (is (= {["DE" 494520] 15000 ["DE" 494521] 3000} (select-keys (:loads l) [["DE" 494520] ["DE" 494521]]))
        "two meters add up and an offset timestamp lands on its UTC hour")
    (is (= 5 (count (:rejects l))))
    (is (some #(re-find #"duplicate load row" (:reason %)) (:rejects l)))
    (is (some #(re-find #"not aligned" (:reason %)) (:rejects l)))
    (is (some #(re-find #"below" (:reason %)) (:rejects l)))
    (is (some #(re-find #"plain decimal" (:reason %)) (:rejects l)))))

(deftest duplicate-certificate-id-is-rejected
  (let [p (hl/validate-policy {})
        s (hl/ingest-supply (tmp-file (str "certificate_id,region,hour,kwh\n"
                                           "X1,DE,2026-06-01T00:00:00Z,5\n"
                                           "X1,DE,2026-06-01T01:00:00Z,5\n")) p)]
    (is (= 1 (count (:certs s))))
    (is (= 1 (count (:rejects s))))))

(deftest strict-hourly-versus-annual-claim
  ;; All solar at noon, all load around the clock: annual style coverage is
  ;; 100 percent while hourly matching is only the noon slice.
  (let [p (hl/validate-policy {})
        loads (into {} (for [s (range 24)] [["DE" s] 1000]))
        certs [{:id "S1" :region "DE" :slot 12 :wh 24000 :g 0.0}]
        mr (hl/match-supply loads certs p)]
    (is (= 1000 (:total-wh mr)))
    (let [wide (hl/validate-policy {:window-before 23 :window-after 23})
          mr2 (hl/match-supply loads certs wide)]
      (is (= 24000 (:total-wh mr2)) "a daily window lets one noon certificate cover the whole day"))))

(deftest run-exit-codes
  (let [load (tmp-file "hour,region,kwh\n2026-06-01T00:00:00Z,DE,10\n")
        supply (tmp-file "certificate_id,region,hour,kwh\nA,DE,2026-06-01T00:00:00Z,5\n")
        grid (tmp-file "hour,region,grid_g_per_kwh,residual_g_per_kwh\n2026-06-01T00:00:00Z,DE,400,500\n")
        base ["--load" load "--supply" supply "--grid" grid]]
    (is (= 0 (:exit (hl/run base))))
    (is (= 1 (:exit (hl/run (concat base ["--min-cfe" "0.9"])))))
    (is (= 0 (:exit (hl/run (concat base ["--min-cfe" "0.5"])))))
    (is (= 2 (:exit (hl/run ["--load" load]))))
    (is (= 2 (:exit (hl/run (concat base ["--bogus" "1"])))))
    (let [bad (tmp-file "hour,region,kwh\n2026-06-01T00:00:00Z,DE,xx\n2026-06-01T01:00:00Z,DE,1\n")]
      (is (= 3 (:exit (hl/run ["--load" bad "--supply" supply "--grid" grid]))))
      (is (= 0 (:exit (hl/run ["--load" bad "--supply" supply "--grid" grid "--max-rejects" "1"])))))
    (let [r (hl/run (concat base ["--format" "json"]))]
      (is (re-find #"\"hourly_cfe\": 0.5" (:out r))))))

(deftest csv-output-defuses-formulas
  (let [out (hl/alloc-csv {:isec 3600
                           :allocations [{:cert {:id "=HYPERLINK(1)" :asset "@x" :region "DE" :slot 0 :g 0.0}
                                          :load-key ["DE" 0] :wh 1000}]})]
    (is (str/includes? out "'=HYPERLINK(1)"))
    (is (str/includes? out "'@x"))))

(let [r (run-tests 'hourly-energy-matching-ledger-test)]
  (System/exit (if (successful? r) 0 1)))
