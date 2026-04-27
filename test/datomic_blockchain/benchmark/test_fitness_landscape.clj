(ns datomic-blockchain.benchmark.test-fitness-landscape
  "Tests for fitness landscape analysis in NK model benchmarking.

   Tests cover:
   - Normalization functions (linear, sigmoid, inverse)
   - Fitness functions (TPS, latency, traceability)
   - Combined fitness calculation
   - Landscape ruggedness analysis
   - Local optima detection
   - Fitness variance
   - Landscape characterization
   - Fitness surface generation
   - Adaptive walk simulation"
  (:require [clojure.test :refer :all]
            [datomic-blockchain.benchmark.nk-model :as nk]
            [datomic-blockchain.benchmark.fitness-landscape :as fl]))

;; =============================================================================
;; Normalization Function Tests
;; =============================================================================

(deftest normalize-linear-in-range-test
  (testing "Linear normalization maps value to [0,1]"
    (is (= 0.0 (fl/normalize-linear 0 0 10)))
    (is (= 1.0 (fl/normalize-linear 10 0 10)))
    (is (= 0.5 (fl/normalize-linear 5 0 10)))))

(deftest normalize-linear-clamping-test
  (testing "Linear normalization clamps to [0,1]"
    (is (= 0.0 (fl/normalize-linear -5 0 10)))
    (is (= 1.0 (fl/normalize-linear 15 0 10)))))

(deftest normalize-linear-equal-bounds-test
  (testing "Linear normalization with equal bounds returns 0.5"
    (is (= 0.5 (fl/normalize-linear 5 5 5)))))

(deftest normalize-sigmoid-center-test
  (testing "Sigmoid normalization returns 0.5 at center"
    (is (= 0.5 (fl/normalize-sigmoid 100 100 0.1)))))

(deftest normalize-sigmoid-above-center-test
  (testing "Sigmoid returns > 0.5 when value > center"
    (is (> (fl/normalize-sigmoid 150 100 0.1) 0.5))))

(deftest normalize-sigmoid-below-center-test
  (testing "Sigmoid returns < 0.5 when value < center"
    (is (< (fl/normalize-sigmoid 50 100 0.1) 0.5))))

(deftest normalize-inverse-test
  (testing "Inverse normalization reverses the scale"
    (is (= 1.0 (fl/normalize-inverse 0 0 10)))   ; Best value
    (is (= 0.0 (fl/normalize-inverse 10 0 10)))  ; Worst value
    (is (= 0.5 (fl/normalize-inverse 5 0 10)))))

;; =============================================================================
;; TPS Fitness Tests
;; =============================================================================

(deftest compute-fitness-tps-high-test
  (testing "High TPS yields high fitness (> 0.9)"
    (let [config (nk/preset-config :small-smooth)]
      (is (> (fl/compute-fitness-tps 500.0 config) 0.9)))))

(deftest compute-fitness-tps-low-test
  (testing "Low TPS yields low fitness (< 0.5)"
    (let [config (nk/preset-config :small-smooth)]
      (is (< (fl/compute-fitness-tps 5.0 config) 0.5)))))

(deftest compute-fitness-tps-bounded-test
  (testing "TPS fitness is bounded to [0,1]"
    (let [config (nk/preset-config :small-smooth)]
      (is (<= 0.0 (fl/compute-fitness-tps 0.0 config) 1.0))
      (is (<= 0.0 (fl/compute-fitness-tps 10000.0 config) 1.0)))))

;; =============================================================================
;; Latency Fitness Tests
;; =============================================================================

(deftest compute-fitness-latency-low-test
  (testing "Low latency yields high fitness (> 0.9)"
    (let [config (nk/preset-config :small-smooth)]
      (is (> (fl/compute-fitness-latency 1.0 config) 0.9)))))

(deftest compute-fitness-latency-high-test
  (testing "High latency yields low fitness (< 0.5)"
    (let [config (nk/preset-config :small-smooth)]
      (is (< (fl/compute-fitness-latency 100.0 config) 0.5)))))

(deftest compute-fitness-latency-bounded-test
  (testing "Latency fitness is bounded to [0,1]"
    (let [config (nk/preset-config :small-smooth)]
      (is (<= 0.0 (fl/compute-fitness-latency 0.0 config) 1.0))
      (is (<= 0.0 (fl/compute-fitness-latency 1000.0 config) 1.0)))))

;; =============================================================================
;; Traceability Fitness Tests
;; =============================================================================

(deftest compute-fitness-traceability-bounded-test
  (testing "Traceability fitness is bounded to [0,1]"
    (let [config (nk/preset-config :small-smooth)
          metrics {:node-count 10 :edge-count 20 :depth 5 :breadth 3}]
      (is (<= 0.0 (fl/compute-fitness-traceability metrics config) 1.0)))))

(deftest compute-fitness-traceability-increases-with-k-test
  (testing "Higher K yields higher traceability fitness"
    (let [smooth-config (nk/preset-config :minimal)
          rugged-config (nk/preset-config :large-rugged)
          metrics {:node-count 10 :edge-count 20 :depth 5 :breadth 3}]
      (is (> (fl/compute-fitness-traceability metrics rugged-config)
             (fl/compute-fitness-traceability metrics smooth-config))))))

;; =============================================================================
;; Combined Fitness Tests
;; =============================================================================

(deftest combined-fitness-bounded-test
  (testing "Combined fitness is bounded to [0,1]"
    (let [config (nk/preset-config :small-smooth)
          metrics {:tps 100.0 :latency 10.0 :traceability 0.5}]
      (is (<= 0.0 (fl/combined-fitness metrics config) 1.0)))))

(deftest combined-fidelity-good-metrics-test
  (testing "Good metrics yield high combined fitness"
    (let [config (nk/preset-config :small-smooth)
          metrics {:tps 400.0 :latency 2.0 :traceability 0.8}]
      (is (> (fl/combined-fitness metrics config) 0.7)))))

(deftest combined-fitness-bad-metrics-test
  (testing "Bad metrics yield low combined fitness"
    (let [config (nk/preset-config :small-smooth)
          metrics {:tps 1.0 :latency 100.0 :traceability 0.1}]
      (is (< (fl/combined-fitness metrics config) 0.5)))))

(deftest combined-fitness-default-metrics-test
  (testing "Combined fitness handles missing metrics with defaults"
    (let [config (nk/preset-config :small-smooth)]
      (is (number? (fl/combined-fitness {} config)))
      (is (number? (fl/combined-fitness {:tps 100.0} config))))))

;; =============================================================================
;; Landscape Ruggedness Tests
;; =============================================================================

(deftest landscape-ruggedness-smooth-test
  (testing "Smooth increasing sequence has high autocorrelation"
    (let [values [0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9]
          ruggedness (fl/landscape-ruggedness values)]
      (is (number? ruggedness))
      (is (> ruggedness 0.5)))))  ; Positive autocorrelation

(deftest landscape-ruggedness-oscillating-test
  (testing "Oscillating sequence has negative autocorrelation"
    (let [values [0.9 0.1 0.9 0.1 0.9 0.1]
          ruggedness (fl/landscape-ruggedness values)]
      (is (< ruggedness 0.0)))))  ; Negative autocorrelation

(deftest landscape-ruggedness-single-value-test
  (testing "Single value returns nil for autocorrelation"
    (is (nil? (fl/landscape-ruggedness [0.5])))))

(deftest landscape-ruggedness-two-values-test
  (testing "Two values can compute autocorrelation"
    (is (number? (fl/landscape-ruggedness [0.3 0.7])))))

(deftest landscape-ruggedness-custom-lag-test
  (testing "Custom lag parameter works"
    (let [values (range 10)
          lag-1 (fl/landscape-ruggedness values 1)
          lag-2 (fl/landscape-ruggedness values 2)]
      (is (number? lag-1))
      (is (number? lag-2)))))

;; =============================================================================
;; Local Optima Tests
;; =============================================================================

(deftest count-local-optima-none-test
  (testing "Monotonically increasing sequence has no local optima"
    (let [values [0.1 0.2 0.3 0.4 0.5]
          result (fl/count-local-optima values)]
      (is (= 0 (:count result)))
      (is (= :none (:type result))))))

(deftest count-local-optima-single-test
  (testing "Single peak detected correctly"
    (let [values [0.1 0.3 0.5 0.3 0.1]
          result (fl/count-local-optima values)]
      (is (= 1 (:count result)))
      (is (= [2] (:positions result)))
      (is (= :single-optimum (:type result))))))

(deftest count-local-optima-multiple-test
  (testing "Multiple peaks detected correctly"
    (let [values [0.1 0.4 0.2 0.5 0.3 0.6 0.2]
          result (fl/count-local-optima values)]
      (is (= 3 (:count result)))
      (is (= :multi-optima (:type result))))))

(deftest count-local-optima-too-few-values-test
  (testing "Less than 3 values returns no optima"
    (let [result-1 (fl/count-local-optima [0.5])
          result-2 (fl/count-local-optima [0.3 0.7])]
      (is (= 0 (:count result-1)))
      (is (= 0 (:count result-2))))))

;; =============================================================================
;; Fitness Variance Tests
;; =============================================================================

(deftest fitness-variance-zero-test
  (testing "Constant sequence has zero variance"
    (let [result (fl/fitness-variance [0.5 0.5 0.5 0.5])]
      (is (= 0.0 (:variance result)))
      (is (= 0.0 (:std-dev result)))
      (is (= 0.5 (:mean result))))))

(deftest fitness-variance-positive-test
  (testing "Varying sequence has positive variance"
    (let [result (fl/fitness-variance [0.1 0.5 0.9])]
      (is (pos? (:variance result)))
      (is (pos? (:std-dev result)))
      (is (number? (:mean result))))))

(deftest fitness-variance-empty-test
  (testing "Empty sequence returns zero values"
    (let [result (fl/fitness-variance [])]
      (is (= 0.0 (:variance result)))
      (is (= 0.0 (:std-dev result))))))

;; =============================================================================
;; Landscape Characterization Tests
;; =============================================================================

(deftest characterize-landscape-structure-test
  (testing "Landscape characterization contains expected keys"
    (let [config (nk/preset-config :small-smooth)
          fitness-map {[:N10 :K1] 0.3 [:N10 :K2] 0.5 [:N10 :K3] 0.4}
          result (fl/characterize-landscape fitness-map config)]
      (is (contains? result :n-total))
      (is (contains? result :k-total))
      (is (contains? result :landscape-type))
      (is (contains? result :fitness-values))
      (is (contains? result :variance))
      (is (contains? result :std-dev))
      (is (contains? result :ruggedness))
      (is (contains? result :count))
      (is (contains? result :type))
      (is (contains? result :theoretical-ruggedness))
      (is (contains? result :theoretical-optima)))))

(deftest characterize-landscape-matches-config-test
  (testing "Characterization N and K match config"
    (let [config (nk/preset-config :small-smooth)
          fitness-map {[:N10 :K1] 0.3 [:N10 :K2] 0.5}
          result (fl/characterize-landscape fitness-map config)]
      (is (= (nk/total-components config) (:n-total result)))
      (is (= (nk/total-interdependencies config) (:k-total result))))))

;; =============================================================================
;; Fitness Surface Generation Tests
;; =============================================================================

(deftest generate-fitness-surface-count-test
  (testing "Fitness surface generates expected number of points"
    (let [n-range [5 10]
          k-range [1 2]
          fitness-fn (fn [n k] (/ n (+ n k)))
          surface (fl/generate-fitness-surface fitness-fn n-range k-range)]
      (is (= 4 (count surface)))  ; 2 × 2
      (is (every? map? surface)))))

(deftest generate-fitness-surface-structure-test
  (testing "Fitness surface points have correct structure"
    (let [fitness-fn (fn [n k] (/ n (+ n k)))
          surface (fl/generate-fitness-surface fitness-fn [10] [5])
          point (first surface)]
      (is (contains? point :n))
      (is (contains? point :k))
      (is (contains? point :fitness))
      (is (= 10 (:n point)))
      (is (= 5 (:k point)))
      (is (number? (:fitness point))))))

(deftest theoretical-fitness-surface-test
  (testing "Theoretical fitness surface generates correctly"
    (let [surface (fl/theoretical-fitness-surface [5 10] [0 5])]
      (is (pos? (count surface)))
      (is (every? number? (map :fitness surface))))))

(deftest theoretical-fitness-surface-decreases-with-k-test
  (testing "Theoretical fitness decreases as K increases"
    (let [surface (fl/theoretical-fitness-surface [50] [1 10 20])
          sorted-by-k (sort-by :k surface)
          fitness-1 (-> sorted-by-k (nth 0) :fitness)
          fitness-10 (-> sorted-by-k (nth 1) :fitness)
          fitness-20 (-> sorted-by-k (nth 2) :fitness)]
      (is (> fitness-1 fitness-10))
      (is (> fitness-10 fitness-20)))))

;; =============================================================================
;; Adaptive Walk Tests
;; =============================================================================

(deftest adaptive-walk-returns-history-test
  (testing "Adaptive walk returns history of steps"
    (let [fitness-fn (fn [config] (:value config))
          start-config {:value 0.5}
          get-neighbors (fn [config]
                          [(-> config (update :value + 0.1))
                           (-> config (update :value - 0.1))])
          result (fl/adaptive-walk fitness-fn start-config get-neighbors 10)]
      (is (vector? result))
      (is (pos? (count result)))
      (is (every? map? result)))))

(deftest adaptive-walk-stops-at-optimum-test
  (testing "Adaptive walk stops when no better neighbor exists"
    (let [fitness-fn (fn [config] (:value config))
          start-config {:value 0.5}
          peak-config {:value 1.0}
          get-neighbors (fn [config]
                          (if (< (:value config) 1.0)
                            [(-> config (update :value + 0.1))]
                            []))
          result (fl/adaptive-walk fitness-fn start-config get-neighbors 100)
          last-step (last result)]
      (is (contains? last-step :status))
      (is (= :local-optimum (:status last-step))))))

(deftest adaptive-walk-max-steps-test
  (testing "Adaptive walk respects max steps"
    (let [fitness-fn (fn [config] (:value config))
          start-config {:value 0}
          get-neighbors (fn [config]
                          [(-> config (update :value inc))])
          result (fl/adaptive-walk fitness-fn start-config get-neighbors 5)]
      (is (<= (count result) 6)))))  ; Initial + 5 steps

;; =============================================================================
;; Utility Function Tests
;; =============================================================================

(deftest export-fitness-data-test
  (testing "Fitness data can be exported to CSV"
    (let [data [{:n 10 :k 1 :fitness 0.5 :landscape-type :smooth}
                {:n 10 :k 2 :fitness 0.3 :landscape-type :moderate}]
          filename "/tmp/test_fitness_export.csv"]
      (fl/export-fitness-data data filename)
      (let [content (slurp filename)]
        (is (.contains content "n,k,fitness,landscape_type"))
        (is (.contains content "10,1,0.500000,smooth"))
        (.delete (java.io.File. filename))))))

;; =============================================================================
;; Edge Cases and Integration Tests
;; =============================================================================

(deftest fitness-with-minimal-config-test
  (testing "Fitness calculations work with minimal K config"
    (let [config (nk/preset-config :minimal)
          metrics {:tps 100.0 :latency 10.0}]
      (is (number? (fl/compute-fitness-tps (:tps metrics) config)))
      (is (number? (fl/compute-fitness-latency (:latency metrics) config)))
      (is (number? (fl/combined-fitness metrics config))))))

(deftest fitness-with-large-config-test
  (testing "Fitness calculations work with large K config"
    (let [config (nk/preset-config :large-rugged)
          metrics {:tps 100.0 :latency 10.0 :traceability 0.8}]
      (is (number? (fl/combined-fitness metrics config)))
      (is (<= 0.0 (fl/combined-fitness metrics config) 1.0)))))

(deftest landscape-type-fitness-correlation-test
  (testing "Smooth landscapes have higher theoretical fitness than rugged"
    (let [smooth-config (nk/preset-config :minimal)
      rugged-config (nk/preset-config :large-rugged)]
      (is (> (nk/expected-ruggedness smooth-config)
             (nk/expected-ruggedness rugged-config))))))
