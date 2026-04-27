(ns datomic-blockchain.benchmark.test-nk-model
  "Tests for NK Model fitness landscape benchmarking.

   Tests cover:
   - NKConfig record creation and validation
   - Complexity metrics calculation
   - Landscape type classification
   - Preset configurations
   - Grid generation
   - Theoretical NK model predictions"
  (:require [clojure.test :refer :all]
            [datomic-blockchain.benchmark.nk-model :as nk])
  (:import [datomic_blockchain.benchmark.nk_model NKConfig]))

;; =============================================================================
;; NKConfig Record Tests
;; =============================================================================

(deftest create-nk-config-test
  (testing "NKConfig record can be created with all parameters"
    (let [config (nk/->NKConfig
                   10
                   4
                   5
                   1
                   2
                   0
                   1)]
      (is (= 10 (:n-entities config)))
      (is (= 4 (:n-participants config)))
      (is (= 5 (:n-batches config)))
      (is (= 1 (:k-qc-points config)))
      (is (= 2 (:k-traceability-hops config)))
      (is (= 0 (:k-certifications config)))
      (is (= 1 (:k-processing-steps config)))))

(deftest map-to-nk-config-test
  (testing "Maps can be converted to NKConfig"
    (let [m {:n-entities 10
              :n-participants 4
              :n-batches 5
              :k-qc-points 1
              :k-traceability-hops 2
              :k-certifications 0
              :k-processing-steps 1}
          config (nk/map->NKConfig m)]
      (is (instance? NKConfig config))
      (is (= 10 (:n-entities config)))))

;; =============================================================================
;; Validation Tests
;; =============================================================================

(deftest validate-valid-config-test
  (testing "Valid NK config passes validation"
    (let [config (nk/preset-config :small-smooth)
          result (nk/validate-nk-config config)]
      (is (true? (:valid result)))
      (is (nil? (:errors result)))))
)

(deftest validate-invalid-entities-test
  (testing "Validation fails when n-entities is not positive"
    (let [config (nk/->NKConfig
                   0
                   3
                   1
                   0
                   1
                   0
                   1)
          result (nk/validate-nk-config config)]
      (is (false? (:valid result)))
      (is (some? (:errors result)))
      (is (some #(re-find #"n-entities" %) (:errors result)))))
)

(deftest validate-invalid-participants-test
  (testing "Validation fails when n-participants is less than 3"
    (let [config (nk/->NKConfig
                   5
                   2
                   1
                   0
                   1
                   0
                   1)
          result (nk/validate-nk-config config)]
      (is (false? (:valid result)))
      (is (some #(re-find #"participants" %) (:errors result)))))
)

(deftest validate-qc-exceeds-entities-test
  (testing "Validation fails when k-qc-points exceeds n-entities"
    (let [config (nk/->NKConfig
                   5
                   3
                   1
                   10
                   1
                   0
                   1)
          result (nk/validate-nk-config config)]
      (is (false? (:valid result)))
      (is (some #(re-find #"k-qc-points" %) (:errors result)))))
)

(deftest validate-hops-exceeds-participants-test
  (testing "Validation fails when k-traceability-hops exceeds n-participants"
    (let [config (nk/->NKConfig
                   5
                   3
                   1
                   0
                   5
                   0
                   1)
          result (nk/validate-nk-config config)]
      (is (false? (:valid result)))
      (is (some #(re-find #"traceability-hops" %) (:errors result)))))
)

(deftest validate-certs-equals-entities-test
  (testing "Validation fails when k-certifications >= n-entities"
    (let [config (nk/->NKConfig
                   5
                   3
                   1
                   0
                   1
                   5
                   1)
          result (nk/validate-nk-config config)]
      (is (false? (:valid result)))
      (is (some #(re-find #"certifications" %) (:errors result)))))
)

(deftest valid-nk-config-predicate-test
  (testing "valid-nk-config? predicate works correctly"
    (let [valid-config (nk/preset-config :small-smooth)
          invalid-config (nk/->NKConfig
                           0
                           3
                           1
                           0
                           1
                           0
                           1)]
      (is (true? (nk/valid-nk-config? valid-config)))
      (is (false? (nk/valid-nk-config? invalid-config)))))
)

;; =============================================================================
;; Complexity Metrics Tests
;; =============================================================================

(deftest total-components-test
  (testing "Total components N is calculated correctly"
    (let [config (nk/->NKConfig
                   10
                   4
                   5
                   1
                   2
                   0
                   1)]
      (is (= 19 (nk/total-components config)))))
)

(deftest total-interdependencies-test
  (testing "Total interdependencies K is calculated correctly"
    (let [config (nk/->NKConfig
                   10
                   4
                   5
                   1
                   2
                   0
                   1)]
      (is (= 4 (nk/total-interdependencies config)))))
)

(deftest complexity-score-test
  (testing "Complexity score uses weighted sum of N and K"
    (let [config (nk/preset-config :small-smooth)]
      (is (number? (nk/complexity-score config)))
      (is (pos? (nk/complexity-score config)))))
)

(deftest complexity-score-increases-with-n-test
  (testing "Complexity score increases with larger N"
    (let [small-n (nk/preset-config :small-smooth)
          large-n (nk/preset-config :large-rugged)]
      (is (> (nk/complexity-score large-n)
             (nk/complexity-score small-n)))))
)

;; =============================================================================
;; Landscape Type Tests
;; =============================================================================

(deftest landscape-type-smooth-test
  (testing "Landscape is smooth when K/N ratio < 0.25"
    (let [config (nk/->NKConfig
                   20
                   5
                   10
                   2
                   2
                   1
                   2)]
      (is (= :smooth (nk/landscape-type config)))))
)

(deftest landscape-type-moderate-test
  (testing "Landscape is moderate when 0.25 <= K/N ratio < 0.5"
    (let [config (nk/preset-config :small-moderate)]
      (is (= :moderate (nk/landscape-type config)))))
)

(deftest landscape-type-rugged-test
  (testing "Landscape is rugged when K/N ratio >= 0.5"
    (let [config (nk/preset-config :medium-rugged)]
      (is (= :rugged (nk/landscape-type config)))))
)

;; =============================================================================
;; Preset Configuration Tests
;; =============================================================================

(deftest preset-minimal-test
  (testing "Minimal preset has correct parameters"
    (let [config (nk/preset-config :minimal)]
      (is (= 5 (:n-entities config)))
      (is (= 3 (:n-participants config)))
      (is (= 0 (:k-qc-points config)))
      (is (= :smooth (nk/landscape-type config)))))
)

(deftest preset-small-smooth-test
  (testing "Small-smooth preset is valid"
    (let [config (nk/preset-config :small-smooth)]
      (is (true? (nk/valid-nk-config? config)))
      (is (= 10 (:n-entities config)))
      (is (= 4 (:n-participants config)))))
)

(deftest preset-all-valid-test
  (testing "All preset configurations are valid"
    (let [presets [:minimal :small-smooth :small-moderate
                    :medium-smooth :medium-rugged :large-rugged]]
      (doseq [preset presets]
        (let [config (nk/preset-config preset)]
          (is (true? (nk/valid-nk-config? config))
              (str preset " should be valid"))))))
)))

;; =============================================================================
;; Scenario ID and Description Tests
;; =============================================================================

(deftest scenario-id-format-test
  (testing "Scenario ID follows expected format"
    (let [config (nk/preset-config :small-smooth)
          id (nk/scenario-id config)]
      (is (string? id))
      (is (.startsWith id "nk_N"))
      (is (some? (re-matches #"nk_N\d+_K\d+_qc\d+_hops\d+_cert\d+_proc\d+" id)))))

(deftest scenario-description-format-test
  (testing "Scenario description is human-readable"
    (let [config (nk/preset-config :small-smooth)
          desc (nk/scenario-description config)]
      (is (string? desc))
      (is (.contains desc "NK Supply Chain")))
)))

;; =============================================================================
;; Grid Generation Tests
;; =============================================================================

(deftest default-nk-grid-size-test
  (testing "Default NK grid produces expected number of configurations"
    (let [grid (nk/default-nk-grid)]
      (is (seq? grid))
      (is (= 24 (count grid)))))  ; 4 N values × 6 K values
)

(deftest nk-grid-custom-test
  (testing "Custom NK grid with specified N and K values"
    (let [grid (nk/nk-grid
                 [5 10]
                 [1 2]
                 (fn [n] (max 3 (quot n 2)))
                 {:k-qc-points 1
                  :k-traceability-hops 1
                  :k-certifications 0
                  :k-processing-steps 1})]
      (is (= 4 (count grid)))  ; 2 × 2
      (is (every? nk/valid-nk-config? grid))))

;; =============================================================================
;; Summary Tests
;; =============================================================================

(deftest summarize-config-test
  (testing "Config summary contains all expected keys"
    (let [config (nk/preset-config :small-smooth)
          summary (nk/summarize-config config)]
      (is (contains? summary :scenario-id))
      (is (contains? summary :description))
      (is (contains? summary :n-total))
      (is (contains? summary :k-total))
      (is (contains? summary :complexity-score))
      (is (contains? summary :landscape-type))
      (is (contains? summary :expected-ruggedness))
      (is (contains? summary :theoretical-optima)))))

(deftest summary-n-total-correct-test
  (testing "Summary n-total equals calculated total components"
    (let [config (nk/preset-config :small-smooth)
          summary (nk/summarize-config config)]
      (is (= (nk/total-components config)
             (:n-total summary)))))

;; =============================================================================
;; Theoretical NK Model Tests
;; =============================================================================

(deftest expected-ruggedness-zero-k-test
  (testing "Expected ruggedness is 1.0 when K=0 (smooth landscape)"
    (let [config (nk/preset-config :minimal)]
      (is (= 1.0 (nk/expected-ruggedness config)))))

(deftest expected-ruggedness-decreases-with-k-test
  (testing "Expected ruggedness decreases as K increases"
    (let [smooth-config (nk/preset-config :minimal)
          rugged-config (nk/preset-config :large-rugged)]
      (is (> (nk/expected-ruggedness smooth-config)
             (nk/expected-ruggedness rugged-config)))))))

(deftest theoretical-optima-smooth-test
  (testing "Smooth landscape (K=0) has single theoretical optimum"
    (let [config (nk/preset-config :minimal)]
      (is (= 1 (nk/theoretical-optima-count config)))))))

(deftest theoretical-optima-increases-with-k-test
  (testing "Number of theoretical optima increases with K (for fixed N)"
    ;; Create two configs with same N but different K values
    (let [smooth-config (nk/map->NKConfig {:n-entities 10
                                            :n-participants 5
                                            :n-batches 5
                                            :k-qc-points 0
                                            :k-traceability-hops 0
                                            :k-certifications 0
                                            :k-processing-steps 0})
          rugged-config (nk/map->NKConfig {:n-entities 10
                                           :n-participants 5
                                           :n-batches 5
                                           :k-qc-points 2
                                           :k-traceability-hops 2
                                           :k-certifications 2
                                           :k-processing-steps 2})]
      ;; Smooth (K=0): 1 optimum, Rugged (K=8): 2^(20/8)=2^2=4 optima
      (is (> (nk/theoretical-optima-count rugged-config)
             (nk/theoretical-optima-count smooth-config)))))))
