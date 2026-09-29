(ns datomic-blockchain.benchmark.test-nk-experiment
  "Tests for the NK completeness experiment: chains that vary with N and
  K are generated as transactable PROV, scored through the public
  completeness interface, and stamped by the rubric."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datomic.api :as d]
            [datomic-blockchain.benchmark.nk-experiment :as nkx]
            [datomic-blockchain.test-support :as ts])
  (:import [java.util UUID]))

(defn- assess-generated-chain
  "Generate one chain for [n k trial] with the given defect rate, seed it,
   and return the completeness assessment of the start entity."
  [conn n k trial defect-rate]
  (let [{:keys [start-entity-id]} (nkx/seed-chain! conn {:n n :k k :trial trial
                                                         :defect-rate defect-rate})]
    (nkx/assess-chain (d/db conn) start-entity-id)))

(use-fixtures :each (fn [f] (f) (ts/retire-all!)))

(deftest generated-chains-are-scoreable-test
  (testing "a clean generated chain (defect rate 0) is a complete
            provenance path: score 1.0, no named defects"
    (let [conn (ts/fresh-conn)
          result (assess-generated-chain conn 5 3 0 0.0)]
      (is (= :assessed (:status result)))
      (is (= 1.0 (:completeness-score result)))
      (is (= [] (:missing-evidence result)))))
  (testing "a fully defective chain (defect rate 1) names evidence and
            scores below 1.0 on every hop"
    (let [conn (ts/fresh-conn)
          result (assess-generated-chain conn 5 3 0 1.0)]
      (is (= :assessed (:status result)))
      (is (seq (:missing-evidence result)))
      (is (< (:completeness-score result) 1.0))))
  (testing "K sets the derivation depth: k=0 is a single hop, k=3 walks
            four entities"
    (let [conn (ts/fresh-conn)]
      (is (= 1 (:path-count (assess-generated-chain conn 5 0 0 0.0))))
      (let [{:keys [start-entity-id]} (nkx/seed-chain! conn {:n 5 :k 3 :trial 0
                                                             :defect-rate 0.0})
            hops (nkx/chain-hop-count (d/db conn) start-entity-id)]
        (is (= 4 hops))))))

(deftest experiment-rows-span-k-and-carry-decisions-test
  (testing "a run over a small grid yields rows with n, k, score, and a
            rubric decision consistent with the named evidence"
    (let [rows (nkx/run-experiment {:n-values [5]
                                    :k-values [1 3]
                                    :trials 4
                                    :defect-rate 0.5
                                    :seed 42})]
      (is (= 8 (count rows)))
      (is (< 1 (count (distinct (map :k rows)))) "more than one K is reported")
      (doseq [row rows]
        (is (contains? row :n))
        (is (contains? row :k))
        (is (contains? row :trial))
        (is (number? (:completeness-score row)))
        (is (<= 0 (:completeness-score row) 1))
        (is (vector? (:missing-evidence row)))
        (is (contains? #{:pull :do-not-pull} (get-in row [:recall-decision :decision])))
        (is (= (if (seq (:missing-evidence row)) :pull :do-not-pull)
               (get-in row [:recall-decision :decision]))
            "the stamp follows the rubric rule"))
      (testing "hardness varies with K: mean score differs across the K values"
        (let [mean (fn [k] (/ (reduce + (map :completeness-score
                                           (filter #(= k (:k %)) rows)))
                              (count (filter #(= k (:k %)) rows))))
              m1 (mean 1)
              m3 (mean 3)]
          (is (not= m1 m3)))))))

;; =============================================================================
;; Run artifacts: raw CSV, summary CSV, manifest
;; =============================================================================

(deftest experiment-outputs-are-written-test
  (testing "a run writes raw rows, a per-K summary CSV, and a manifest
            recording command, environment, timestamp, and commit"
    (let [out-dir (str "/tmp/nk-out-" (UUID/randomUUID))
          {:keys [rows summary manifest]} (nkx/write-experiment-outputs!
                                           {:n-values [5]
                                            :k-values [1 3]
                                            :trials 3
                                            :defect-rate 0.5
                                            :seed 42}
                                           out-dir)
          read-csv (fn [f]
                     (with-open [r (clojure.java.io/reader (str out-dir "/" f))]
                       (doall (clojure.data.csv/read-csv r))))
          raw (read-csv "nk-completeness-raw.csv")
          sum-csv (read-csv "nk-completeness-summary.csv")
          manifest (read-string (slurp (str out-dir "/manifest.edn")))]
      (is (= 6 (count rows)))
      (is (= ["n" "k" "trial" "completeness-score" "missing-evidence" "recall-decision"]
             (first raw)))
      (is (= ["n" "k" "trials" "mean-completeness-score" "do-not-pull-rate"
              "mean-missing-evidence-count"]
             (first sum-csv)))
      (is (< 1 (count (distinct (rest (map second sum-csv))))) "summary spans >1 K")
      (is (string? (:command manifest)))
      (is (map? (:environment manifest)))
      (is (string? (:timestamp manifest)))
      (is (string? (:commit manifest)))
      (is (map? (:opts manifest))))))
