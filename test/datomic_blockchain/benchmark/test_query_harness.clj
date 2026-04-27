(ns datomic-blockchain.benchmark.test-query-harness
  "Regression tests for benchmark harness route definitions."
  (:require [clojure.string :as str]
            [clojure.test :refer :all]))

(deftest query-harness-uses-public-qr-route-test
  (testing "Q1 benchmark calls the explicit public QR route"
    (let [harness (slurp "benchmarks/main-revised/query/run_query_harness.bash")]
      (is (str/includes? harness "\"/api/trace/qr/${QR_CODE}\""))
      (is (not (str/includes? harness "\"/api/trace/${QR_CODE}\""))))))
