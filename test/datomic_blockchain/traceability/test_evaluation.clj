(ns datomic-blockchain.traceability.test-evaluation
  "Tests for the labeled-path evaluation: the checkable harness that
  records completeness score, missing evidence, and recall decision per
  labeled path, and checks the score's agreement with the rubric."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datomic.api :as d]
            [datomic-blockchain.test-support :as ts]
            [datomic-blockchain.traceability.evaluation :as evaluation]
            [datomic-blockchain.traceability.recall-rubric :as recall-rubric]))

(use-fixtures :each (fn [f] (f) (ts/retire-all!)))

(deftest evaluation-records-score-evidence-and-decision-per-path-test
  (testing "each labeled path yields a row with score, named evidence,
            and the rubric stamp"
    (let [conn (ts/fresh-conn)
          result (evaluation/evaluate-labeled-paths! conn)
          rows (:rows result)
          by-label (into {} (map (juxt :label identity) rows))]
      (is (= 5 (count rows)))
      (doseq [row rows]
        (is (contains? row :completeness-score))
        (is (vector? (:missing-evidence row)))
        (is (contains? (:recall-decision row) :decision)))
      (is (= :do-not-pull (get-in by-label [:complete :recall-decision :decision])))
      (is (= [] (get-in by-label [:complete :missing-evidence])))
      (is (= 1.0 (get-in by-label [:complete :completeness-score]))))))

(deftest evaluation-checks-score-agrees-with-labels-test
  (testing "the agreement checks: complete scores above every defective
            path, expected evidence is named, and do-not-pull outranks pull"
    (let [conn (ts/fresh-conn)
          result (evaluation/evaluate-labeled-paths! conn)
          checks (:checks result)]
      (is (true? (:complete-scores-above-defective? checks)))
      (is (true? (:expected-evidence-named? checks)))
      (is (true? (:ranking-matches-decisions? checks)))
      (is (true? (:all-agree? checks))))))
