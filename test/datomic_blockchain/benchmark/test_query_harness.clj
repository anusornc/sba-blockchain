(ns datomic-blockchain.benchmark.test-query-harness
  "Regression tests for benchmark harness route definitions."
  (:require [clojure.string :as str]
            [clojure.test :refer :all]))

(def harness-paths
  ["benchmarks/current/uht-query/run_query_harness.bash"
   "benchmarks/main-revised/query/run_query_harness.bash"
   "benchmarks/reproducibility/query/run_query_harness.bash"])

(defn- existing-harness-path []
  (some #(when (.exists (java.io.File. %)) %) harness-paths))

(deftest query-harness-uses-public-qr-route-test
  (testing "Q1 benchmark calls the explicit public QR route"
    (let [harness-path (existing-harness-path)]
      (is harness-path "Expected a query benchmark harness in the private or public layout")
      (when harness-path
        (let [harness (slurp harness-path)]
          (is (str/includes? harness "\"/api/trace/qr/${QR_CODE}\""))
          (is (not (str/includes? harness "\"/api/trace/${QR_CODE}\""))))))))
