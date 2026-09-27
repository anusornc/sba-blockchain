(ns datomic-blockchain.traceability.test-recall-rubric
  "Tests for the recall rubric and the labeled-path fixtures.

  The rubric stamps pull / do-not-pull from named evidence defects —
  deliberately independent of the completeness score, whose agreement
  with these labels is ticket 03's evaluation."
  (:require [clojure.test :refer [deftest is testing]]
            [datomic.api :as d]
            [datomic-blockchain.datomic.schema :as schema]
            [datomic-blockchain.traceability.recall-rubric :as recall-rubric]
            [datomic-blockchain.traceability.sample-data :as sample])
  (:import [java.util UUID]))

(defn- fresh-conn
  []
  (let [uri (str "datomic:mem://recall-rubric-test-" (UUID/randomUUID))]
    (d/delete-database uri)
    (d/create-database uri)
    (let [conn (d/connect uri)]
      @(d/transact conn schema/full-schema)
      conn)))

;; =============================================================================
;; The written rubric, as data
;; =============================================================================

(deftest rubric-stamps-every-defect-type-test
  (testing "the rubric maps each defect type to a pull decision and the
            complete path to do-not-pull, each with a written rationale"
    (let [complete (recall-rubric/decision-for :complete)]
      (is (= :do-not-pull (:decision complete)))
      (is (string? (:rationale complete))))
    (doseq [defect (sample/labeled-path-defects)]
      (let [stamped (recall-rubric/decision-for defect)]
        (is (= :pull (:decision stamped)) (str defect " stamps pull"))
        (is (string? (:rationale stamped)) (str defect " carries a rationale"))))))

;; =============================================================================
;; Labeled paths load as fixtures
;; =============================================================================

(deftest labeled-paths-load-as-fixtures-test
  (testing "one do-not-pull negative control plus one pull path per defect
            type, each addressable in the database by its own QR"
    (let [conn (fresh-conn)
          paths (recall-rubric/labeled-paths! conn)
          db (d/db conn)
          by-label (into {} (map (juxt :label identity) paths))]
      (is (= 5 (count paths)))
      (is (= :do-not-pull (get-in by-label [:complete :recall-decision :decision])))
      (doseq [defect (sample/labeled-path-defects)]
        (let [record (get by-label defect)]
          (is (some? record) (str defect " has a labeled path"))
          (is (= :pull (get-in record [:recall-decision :decision])))
          (is (= (sample/expected-missing-evidence defect)
                 (:expected-missing-evidence record))))
        (let [qr (get-in by-label [defect :anchors :qr-code])]
          (is (some? (d/q '[:find ?e .
                            :in $ ?qr
                            :where [?e :traceability/qr-code ?qr]]
                          db qr))
              (str defect " fixture is addressable by QR"))))
      (let [qrs (map (comp :qr-code :anchors) paths)]
        (is (= (count qrs) (count (distinct qrs))) "fixtures are individually addressable")))))
