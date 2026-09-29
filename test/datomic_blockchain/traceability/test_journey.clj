(ns datomic-blockchain.traceability.test-journey
  "Tests for the Journey read model: the two named adapters and the
   canonical Stage (ADR-0003)."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datomic-blockchain.data.dataset-loader :as dataset]
            [datomic-blockchain.traceability.journey :as journey]
            [datomic-blockchain.traceability.sample-data :as sample]
            [datomic-blockchain.test-support :as ts]))

(deftest stages-from-dataset-name-variant-specific-manufacturing-test
  (testing "each Variant's manufacturing Stage names its own processing
            activity from the Deterministic Dataset"
    (let [ds (dataset/load-dataset)]
      (doseq [[variant label] [[:chocolate "Chocolate UHT Processing"]
                               [:plain "Plain UHT Processing"]
                               [:strawberry "Strawberry UHT Processing"]]]
        (let [stages (journey/stages-from-dataset ds variant)]
          (is (= 4 (count stages)) (str variant " has four stages"))
          (is (= [:farm :manufacturing :logistics :retail] (mapv :stage stages)))
          (is (= label (-> stages second :activity :rdfs/label))
              (str variant " names its own processing"))))))
  (testing "raw-milk falls back to the generic processing activity"
    (let [stages (journey/stages-from-dataset (dataset/load-dataset) :raw-milk)]
      (is (= "UHT Processing and Packaging"
             (-> stages second :activity :rdfs/label)))))
  (testing "an unknown Variant has no Journey"
    (is (nil? (journey/stages-from-dataset (dataset/load-dataset) :no-such-variant)))))

(deftest stages-from-ledger-returns-time-ordered-canonical-stages-test
  (testing "the Ledger adapter maps history tuples to the canonical Stage
            with the acting agent, ordered by time"
    (ts/with-fresh-db [conn]
      (let [{:keys [benchmark-anchors]} (sample/seed-uht-sample! conn)
            db (datomic.api/db conn)
            entity-uuid (java.util.UUID/fromString (:entity-id benchmark-anchors))
            stages (journey/stages-from-ledger db entity-uuid)
            times (map :time stages)]
        (is (= (sort times) times) "stages are time-ordered")
        (is (pos? (count stages)))
        (doseq [s stages]
          (is (contains? s :stage))
          (is (contains? s :activity))
          (is (contains? s :agent)))))))

(use-fixtures :each (fn [f] (f) (ts/retire-all!)))
