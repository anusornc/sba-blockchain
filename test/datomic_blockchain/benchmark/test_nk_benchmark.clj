(ns datomic-blockchain.benchmark.test-nk-benchmark
  "Regression tests for NK benchmark metric semantics."
  (:require [clojure.test :refer :all]
            [datomic-blockchain.benchmark.nk-benchmark :as nk-bench]
            [datomic.api :as d]))

(deftest nk-throughput-separates-transactions-from-entity-write-rate-test
  (testing "TPS counts committed transactions; entity write rate counts payload entities"
    (let [config (nk-bench/map->NKConfig {:n-entities 10
                                          :n-participants 3
                                          :n-batches 1
                                          :k-qc-points 1
                                          :k-traceability-hops 2
                                          :k-certifications 0
                                          :k-processing-steps 1})]
      (with-redefs [d/transact (fn [_ _]
                                 (Thread/sleep 10)
                                 (delay {:db-after :ok}))]
        (let [result (#'nk-bench/run-single-benchmark nil config 1 0)
              transaction-tps (get-in result [:transaction-tps :mean])
              entity-write-rate (get-in result [:entity-write-rate :mean])]
          (is (pos? transaction-tps))
          (is (pos? entity-write-rate))
          (is (> entity-write-rate transaction-tps))
          (is (= transaction-tps (get-in result [:tps :mean]))))))))
