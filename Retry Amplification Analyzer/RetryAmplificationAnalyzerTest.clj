(ns retry-amplification-analyzer-test
  (:require [clojure.test :refer [deftest is testing run-tests]]
            [clojure.java.io :as io]))

(load-file (.getPath (io/file (or (System/getProperty "analyzer.dir") ".") "RetryAmplificationAnalyzer.clj")))
(require '[retry-amplification-analyzer :as ra])

(defn- chain [& retries]
  (let [names (map #(keyword (str "s" %)) (range (inc (count retries))))]
    {:entry :s0 :entry-rps 10
     :services (into {} (map (fn [n r nxt]
                               [n (if nxt
                                    {:calls [{:to nxt :timeout-ms 100 :retries r :idempotent true :jitter true}]}
                                    {})])
                             names (concat retries [nil]) (concat (rest names) [nil])))}))

(defn- with-out-str-code [f] (let [code (volatile! nil)] (with-out-str (vreset! code (f))) @code))

(defn- ids [report] (set (map :id (:findings report))))

(deftest amplification-multiplies-across-layers
  (let [r (ra/analyze (chain 2 3))
        leaf (last (:nodes r))]
    (is (= 12.0 (:amplification leaf)))
    (is (= 120.0 (:storm-rps leaf)))
    (is (= 10.0 (:normal-rps leaf)))
    (is (contains? (ids r) :amplification-high))))

(deftest budget-caps-the-storm
  (let [t (-> (chain 3)
              (assoc-in [:services :s0 :calls 0 :budget-ratio] 0.1))
        leaf (last (:nodes (ra/analyze t)))]
    (is (< (Math/abs (- 11.0 (:budgeted-rps leaf))) 1e-9))
    (is (= 40.0 (:storm-rps leaf)))))

(deftest retry-storm-overload-is-critical
  (let [t (-> (chain 3) (assoc-in [:services :s1 :capacity-rps] 20))
        r (ra/analyze t)]
    (is (contains? (ids r) :retry-storm-overload))
    (is (= :critical (:max-severity r)))))

(deftest budget-dependency-is-called-out
  (let [t (-> (chain 3)
              (assoc-in [:services :s0 :calls 0 :budget-ratio] 0.1)
              (assoc-in [:services :s1 :capacity-rps] 20))]
    (is (contains? (ids (ra/analyze t)) :depends-on-retry-budget))
    (is (not (contains? (ids (ra/analyze t)) :retry-storm-overload)))))

(deftest deadline-inversion
  (let [r (ra/analyze (chain 0 5))]
    (is (contains? (ids r) :deadline-inversion))
    (is (not (contains? (ids (ra/analyze (chain 0))) :deadline-inversion)))))

(deftest backoff-respects-the-cap
  (is (= (+ 100.0 200.0 300.0 300.0)
         (ra/backoff-total-ms {:retries 4 :backoff-ms 100.0 :backoff-cap-ms 300.0}))))

(deftest cycles-are-reported-not-looped
  (let [t {:entry :a :entry-rps 1
           :services {:a {:calls [{:to :b :timeout-ms 10}]}
                      :b {:calls [{:to :a :timeout-ms 10}]}}}
        r (ra/analyze t)]
    (is (contains? (ids r) :cycle))
    (is (= :critical (:max-severity r)))))

(deftest policy-smells
  (let [t {:entry :a :entry-rps 1
           :services {:a {:calls [{:to :b :timeout-ms 100 :retries 2 :idempotent false}]}
                      :b {}}}
        got (ids (ra/analyze t))]
    (is (every? got [:non-idempotent-retry :no-jitter :no-retry-budget :zero-backoff]))))

(deftest validation-collects-every-problem
  (let [r (ra/analyze {:entry :zzz :entry-rps -1
                       :services {:a {:calls [{:to :nope :retries -1}]}}})]
    (testing "errors instead of a thrown exception"
      (is (> (count (:errors r)) 3)))))

(deftest unreachable-services-are-informational
  (let [t (assoc-in (chain 0) [:services :ghost] {})]
    (is (contains? (ids (ra/analyze t)) :unreachable))))

(deftest exit-codes
  (let [dir (or (System/getProperty "analyzer.dir") ".")
        f (str (io/file dir "ExampleTopology.edn"))]
    (is (= 1 (with-out-str-code (fn [] (ra/run [f])))))
    (is (= 1 (with-out-str-code (fn [] (ra/run [f "--fail-on" "critical"])))))
    (let [clean (java.io.File/createTempFile "clean" ".edn")]
      (spit clean (pr-str {:entry :a :entry-rps 1 :services {:a {:calls [{:to :b :timeout-ms 100}]} :b {}}}))
      (is (= 0 (with-out-str-code (fn [] (ra/run [(str clean)]))))))
    (is (= 2 (with-out-str-code (fn [] (binding [*err* (java.io.StringWriter.)] (ra/run ["missing.edn"]))))))))


(let [{:keys [fail error]} (run-tests 'retry-amplification-analyzer-test)]
  (System/exit (if (zero? (+ fail error)) 0 1)))
