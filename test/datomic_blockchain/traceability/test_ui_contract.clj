(ns datomic-blockchain.traceability.test-ui-contract
  "Tests for the Trace View contract module: fixture-dataset injection,
   the frozen wire shapes, and the vocabulary accessor (ADR-0002)."
  (:require [clojure.test :refer [deftest is testing]]
            [datomic-blockchain.traceability.ui-contract :as ui-contract]))

(def ^:private fixture-ds
  "A minimal Deterministic Dataset: one chocolate batch and the four
   Journey activities, small enough to assert exact shapes against."
  {:meta {:dataset "fixture"}
   :products {:chocolate {:traceability/batch "UHT-CHOC-FX-001"
                          :traceability/product "UHT Chocolate Milk"
                          :uht/qr-code "UHT-CHOC-FX-001-QR"
                          :uht/variant-name "Chocolate"
                          :uht/flavor "chocolate"
                          :rdfs/label "UHT Chocolate Milk"}
              :raw-milk {:traceability/batch "RAW-MILK-FX-001"
                         :rdfs/label "Raw Milk"}}
   :agents {:farmer {:prov/agent-name "Fixture Farm"}}
   :activities {:milking {:rdfs/label "Milking"
                          :uht/location {:facility "Fixture Farm"}
                          :prov/startedAtTime "2024-01-01T08:00:00Z"}
                :uht-chocolate-processing {:rdfs/label "Chocolate UHT Processing"
                                           :uht/location {:facility "Fixture Plant"}
                                           :prov/startedAtTime "2024-01-01T09:00:00Z"}
                :transport {:rdfs/label "Cold Chain Transport"
                            :uht/location {:start "Plant" :end "Store"}
                            :prov/startedAtTime "2024-01-01T10:00:00Z"}
                :retail-sale {:rdfs/label "Retail Sale"
                              :uht/location {:store "Fixture Store" :shelf "A1"}
                              :prov/startedAtTime "2024-01-01T11:00:00Z"}}})

(deftest build-trace-view-projects-a-fixture-dataset-test
  (testing "an injected fixture dataset drives the whole Trace View — the
            module has no classpath dependency at this seam"
    (let [view (ui-contract/build-trace-view fixture-ds "UHT-CHOC-FX-001-QR" "qr")]
      (is (= "2026-10-01" (:contract-version view)))
      (is (= "deterministic-uht-dataset" (:source view)))
      (is (= "UHT-CHOC-FX-001" (get-in view [:product :batch])))
      (is (= "UHT Chocolate Milk" (get-in view [:product :product])))
      (is (= 4 (count (:stages view))))
      (is (= "Milking" (:activity (first (:stages view)))))
      (is (= ["farm" "manufacturing" "logistics" "retail"]
             (mapv :stage (:stages view))))
      ;; manufacturing names the Variant's own processing activity
      (is (= "Chocolate UHT Processing"
             (:activity (second (:stages view)))))
      ;; graph: product + qr nodes, 4 activity nodes, 4 location nodes
      (is (= 10 (count (get-in view [:graph :nodes]))))
      ;; edges: 4 stage + 4 location + 1 qr
      (is (= 9 (count (get-in view [:graph :edges])))))))

(deftest build-trace-view-looks-up-by-batch-test
  (testing "kind=batch resolves through the same fixture seam"
    (let [view (ui-contract/build-trace-view fixture-ds "UHT-CHOC-FX-001" "batch")]
      (is (= "UHT-CHOC-FX-001" (get-in view [:product :batch]))))))

(deftest build-trace-view-rejects-bad-input-test
  (testing "error modes stay on the contract: 400 ex-info shapes"
    (is (thrown-with-msg? Exception #"Unsupported trace kind"
                          (ui-contract/build-trace-view fixture-ds "X" "rfid")))
    (is (thrown-with-msg? Exception #"Trace product not found"
                          (ui-contract/build-trace-view fixture-ds "NOPE-QR" "qr")))))

(deftest build-trace-view-default-arity-loads-canonical-dataset-test
  (testing "the 1-arity serves the canonical classpath dataset unchanged"
    (let [view (ui-contract/build-trace-view "UHT-CHOC-2024-001-QR" "qr")]
      (is (= "2026-10-01" (:contract-version view)))
      (is (= "UHT-CHOC-CM-2024-001" (get-in view [:product :batch])))
      (is (= 4 (count (:stages view)))))))

(deftest ontology-vocabulary-shape-test
  (testing "the vocabulary shares the trace view's contract-version"
    (let [vocab (ui-contract/ontology-vocabulary)]
      (is (= (:contract-version vocab) ui-contract/contract-version))
      (is (= "2026-10-01" (:contract-version vocab)))
      (is (contains? (:prefixes vocab) :prov))
      (is (= 6 (count (:classes vocab))))
      (is (seq (:properties vocab))))))
