(ns datomic-blockchain.benchmark.fitness-landscape
  "Fitness Landscape Analysis for NK Model-Based Supply Chain Benchmarking.

   Fitness Landscape Theory (Kauffman, 1993):
   - Each point in the landscape represents a system configuration (N,K)
   - Height at each point = fitness value (performance metric)
   - K=0: Smooth 'Mount Fuji' landscape (single global optimum)
   - K=N-1: Rugged landscape (many local optima, nearly random)

   This module provides:
   1. Fitness functions for blockchain metrics (TPS, latency, traceability)
   2. Landscape analysis (ruggedness, local optima)
   3. Fitness landscape visualization data generation"
  (:require [datomic-blockchain.benchmark.nk-model :as nk]
            [clojure.math :as math]))

;; =============================================================================
;; Fitness Function Configuration
;; =============================================================================

(def ^:private fitness-parameters
  "Baseline and target values for fitness normalization.

   Based on benchmark results from docs/benchmark/BENCHMARK_RESULTS.md:
   - Datomic: 467.94 TPS, 1.89ms latency
   - Ethereum: ~10 TPS, ~100ms latency
   - Neo4j: 99.48 TPS, 9.72ms latency"
  {:tps
   {:baseline 1.0          ; Minimum acceptable TPS (1 transaction/sec)
    :target 500.0          ; Target TPS (based on Datomic performance)
    :weight 1.0}           ; Weight in combined fitness

   :latency
   {:baseline 100.0        ; Maximum acceptable latency (ms)
    :target 1.0            ; Target latency (ms, based on Datomic)
    :weight 0.8}           ; Weight in combined fitness (lower is better)

   :traceability
   {:baseline 0.0          ; Minimum traceability score
    :target 1.0            ; Target traceability (normalized 0-1)
    :weight 0.6}           ; Weight in combined fitness

   :complexity-penalty
   {:factor 0.001          ; Penalty per unit of complexity (reduced for balanced fitness)
    :weight 0.2}})         ; Weight in combined fitness

;; =============================================================================
;; Normalization Functions
;; =============================================================================

(defn normalize-linear
  "Normalize value to [0,1] using linear scaling.

   Parameters:
   - value: Measured value
   - min-val: Minimum expected value (maps to 0)
   - max-val: Maximum expected value (maps to 1)

   Returns: Normalized value in [0,1]"
  [value min-val max-val]
  (if (= max-val min-val)
    0.5
    (clojure.core/max 0.0 (clojure.core/min 1.0 (double (/ (- value min-val) (- max-val min-val)))))))

(defn normalize-sigmoid
  "Normalize value using sigmoid function for smooth transitions.

   Parameters:
   - value: Measured value
   - center: Center point (maps to 0.5)
   - steepness: Steepness of transition

   Returns: Normalized value in [0,1]"
  [value center steepness]
  (/ 1.0 (+ 1.0 (math/exp (* (- steepness) (- value center))))))

(defn normalize-inverse
  "Normalize value where lower is better (e.g., latency).

   Parameters:
   - value: Measured value (lower is better)
   - min-val: Minimum expected value (maps to 1)
   - max-val: Maximum expected value (maps to 0)

   Returns: Normalized value in [0,1]"
  [value min-val max-val]
  (- 1.0 (normalize-linear value min-val max-val)))

;; =============================================================================
;; Fitness Functions
;; =============================================================================

(defn compute-fitness-tps
  "Compute fitness score for transactions per second.

   Higher TPS = higher fitness. Uses sigmoid normalization
   to handle wide range of TPS values (1 to 1000+).

   Parameters:
   - measured-tps: Actual measured TPS
   - config: NKConfig for context (optional)

   Returns: Fitness in [0,1]"
  [measured-tps ^datomic_blockchain.benchmark.nk_model.NKConfig config]
  (let [params (get fitness-parameters :tps)
        baseline (:baseline params)
        target (:target params)]
    ;; Use sigmoid for smooth saturation at high TPS
    (normalize-sigmoid measured-tps
                      (/ (+ baseline target) 2.0)
                      (/ 1.0 baseline))))

(defn compute-fitness-latency
  "Compute fitness score for transaction latency.

   Lower latency = higher fitness. Uses inverse normalization.

   Parameters:
   - measured-latency: Actual measured latency (ms)
   - config: NKConfig for context (optional)

   Returns: Fitness in [0,1]"
  [measured-latency ^datomic_blockchain.benchmark.nk_model.NKConfig config]
  (let [params (get fitness-parameters :latency)
        baseline (:baseline params)
        target (:target params)]
    (normalize-inverse measured-latency target baseline)))

(defn compute-fitness-traceability
  "Compute fitness score for traceability capability.

   Traceability fitness is based on:
   - K parameters: Higher K = more interdependencies = better traceability
   - N parameters: More entities = richer provenance graph

   Parameters:
   - graph-metrics: Map with :node-count, :edge-count, :depth, :breadth
   - config: NKConfig

   Returns: Fitness in [0,1]"
  [graph-metrics ^datomic_blockchain.benchmark.nk_model.NKConfig config]
  (let [params (get fitness-parameters :traceability)
        target (:target params)

        ;; Traceability score based on K (interdependencies)
        k-total (nk/total-interdependencies config)
        k-max (* 2 (nk/total-components config))  ; Theoretical max

        ;; Graph depth contributes to traceability
        depth-score (normalize-linear (or (:depth graph-metrics) 1) 1 10)

        ;; Combine K and depth
        k-score (min 1.0 (/ k-total k-max))]
    (* (+ k-score depth-score) 0.5)))

(defn combined-fitness
  "Compute combined fitness score from multiple metrics.

   Weighted sum of individual fitness functions with complexity penalty.

   Parameters:
   - metrics: Map with :tps, :latency, :traceability
   - config: NKConfig

   Returns: Combined fitness in [0,1]"
  [metrics ^datomic_blockchain.benchmark.nk_model.NKConfig config]
  (let [tps-fitness (compute-fitness-tps (:tps metrics 1.0) config)
        latency-fitness (compute-fitness-latency (:latency metrics 100.0) config)
        traceability-fitness (compute-fitness-traceability (:traceability metrics {}) config)

        tps-params (get fitness-parameters :tps)
        latency-params (get fitness-parameters :latency)
        traceability-params (get fitness-parameters :traceability)

        weights [(:weight tps-params)
                 (:weight latency-params)
                 (:weight traceability-params)]
        fitness-values [tps-fitness latency-fitness traceability-fitness]

        total-weight (reduce + weights)
        weighted-sum (reduce + (map * weights fitness-values))

        base-fitness (/ weighted-sum total-weight)

        ;; Apply complexity penalty (higher complexity reduces fitness slightly)
        penalty-params (get fitness-parameters :complexity-penalty)
        complexity (nk/complexity-score config)
        penalty (* (:factor penalty-params) complexity)]

    (max 0.0 (- base-fitness penalty))))

;; =============================================================================
;; Landscape Analysis
;; =============================================================================

(defn landscape-ruggedness
  "Calculate landscape ruggedness using autocorrelation.

   Ruggedness measures how fitness values correlate with nearby configurations.
   - High autocorrelation (near 1.0) = smooth landscape (Mount Fuji)
   - Low autocorrelation (near 0.0) = rugged landscape (many local optima)
   - Negative autocorrelation = chaotic landscape

   Parameters:
   - fitness-values: Vector of fitness values ordered by configuration similarity
   - lag: Distance for autocorrelation (default 1)

   Returns: Autocorrelation coefficient in [-1, 1]"
  ([fitness-values]
   (landscape-ruggedness fitness-values 1))
  ([fitness-values lag]
   (when (> (count fitness-values) 1)
     (let [n (count fitness-values)
           mean (/ (reduce + fitness-values) n)
           variance (/ (reduce + (map #(math/pow (- % mean) 2) fitness-values)) n)

           ;; Autocorrelation at lag
           lagged-pairs (map vector
                             (take (- n lag) fitness-values)
                             (drop lag fitness-values))
           covariance (/ (reduce + (map (fn [[x y]]
                                          (* (- x mean) (- y mean)))
                                        lagged-pairs))
                         (- n lag))

           autocorr (if (zero? variance)
                      0.0
                      (/ covariance variance))]
       autocorr))))

(defn count-local-optima
  "Count local optima in a fitness landscape.

   A point is a local optimum if it's higher than all neighbors.

   Parameters:
   - fitness-values: Vector of fitness values

   Returns: Map with :count, :positions, :type"
  [fitness-values]
  (let [n (count fitness-values)]
    (if (< n 3)
      {:count 0 :positions [] :type :none}
      (let [optima-positions
            (for [i (range 1 (dec n))
                  :when (and (> (nth fitness-values i) (nth fitness-values (dec i)))
                             (> (nth fitness-values i) (nth fitness-values (inc i))))]
              i)]
        {:count (count optima-positions)
         :positions optima-positions
         :type (cond
                 (zero? (count optima-positions)) :none
                 (= (count optima-positions) 1) :single-optimum
                 :else :multi-optima)}))))

(defn fitness-variance
  "Calculate variance of fitness values.

   Higher variance = more diverse performance across configurations.
   Lower variance = consistent performance.

   Parameters:
   - fitness-values: Vector of fitness values

   Returns: Variance and standard deviation"
  [fitness-values]
  (let [n (count fitness-values)]
    (if (zero? n)
      {:variance 0.0 :std-dev 0.0}
      (let [mean (/ (reduce + fitness-values) n)
            variance (/ (reduce + (map #(math/pow (- % mean) 2) fitness-values)) n)
            std-dev (math/sqrt variance)]
        {:variance variance :std-dev std-dev :mean mean}))))

;; =============================================================================
;; NK Landscape Characterization
;; =============================================================================

(defn characterize-landscape
  "Full landscape characterization for NK configuration.

   Parameters:
   - fitness-map: Map of N x K -> fitness values
   - config: NKConfig

   Returns: Comprehensive landscape analysis"
  [fitness-map ^datomic_blockchain.benchmark.nk_model.NKConfig config]
  (let [fitness-values (vec (vals fitness-map))
        variance-data (fitness-variance fitness-values)
        ruggedness (landscape-ruggedness fitness-values)
        optima (count-local-optima fitness-values)
        landscape-type (nk/landscape-type config)]

    (merge
     {:n-total (nk/total-components config)
      :k-total (nk/total-interdependencies config)
      :landscape-type landscape-type
      :fitness-values fitness-values
      :fitness-count (count fitness-values)}
     variance-data
     {:ruggedness ruggedness}
     optima
     {:theoretical-ruggedness (nk/expected-ruggedness config)
      :theoretical-optima (nk/theoretical-optima-count config)})))

;; =============================================================================
;; Fitness Surface Generation (for 3D visualization)
;; =============================================================================

(defn generate-fitness-surface
  "Generate fitness surface data for 3D visualization.

   Creates a grid of (N, K, fitness) points for plotting.

   Parameters:
   - fitness-fn: Function that takes [n k] and returns fitness
   - n-range: Range of N values
   - k-range: Range of K values

   Returns: Vector of {:n, :k, :fitness} maps"
  [fitness-fn n-range k-range]
  (for [n n-range
        k k-range]
    {:n n
     :k k
     :fitness (fitness-fn n k)}))

(defn theoretical-fitness-surface
  "Generate theoretical fitness surface based on NK model.

   Uses Kauffman's formula: fitness ~ (1 - K/N) for random landscapes.

   Parameters:
   - n-range: Range of N values
   - k-range: Range of K values
   - base-fitness: Base fitness level (default 0.5)

   Returns: Vector of {:n, :k, :fitness} maps"
  ([n-range k-range]
   (theoretical-fitness-surface n-range k-range 0.5))
  ([n-range k-range base-fitness]
   (generate-fitness-surface
    (fn [n k]
      (if (zero? n)
        base-fitness
        (* base-fitness (+ 0.2 (* 0.8 (max 0.0 (- 1.0 (/ k n))))))))
    n-range
    k-range)))

;; =============================================================================
;; Adaptive Walk Simulation
;; =============================================================================

(defn adaptive-walk
  "Simulate an adaptive walk on the fitness landscape.

   Starting from a random configuration, always move to the fitter neighbor.
   Simulates how a system might evolve under selection pressure.

   Parameters:
   - fitness-fn: Function taking configuration -> fitness
   - start-config: Starting configuration
   - get-neighbors: Function returning neighbor configurations
   - max-steps: Maximum steps to take (default 100)

   Returns: Walk history with [:step :config :fitness]"
  ([fitness-fn start-config get-neighbors]
   (adaptive-walk fitness-fn start-config get-neighbors 100))
  ([fitness-fn start-config get-neighbors max-steps]
   (loop [current-config start-config
          current-fitness (fitness-fn start-config)
          step 0
          history [{:step 0 :config start-config :fitness current-fitness}]]
     (if (>= step max-steps)
       history
       (let [neighbors (get-neighbors current-config)
              neighbor-fitness (map (fn [n] {:config n :fitness (fitness-fn n)}) neighbors)
              best-neighbor (first (sort-by :fitness > neighbor-fitness))
              improved? (and best-neighbor (> (:fitness best-neighbor) current-fitness))]
         (if improved?
           (recur (:config best-neighbor)
                  (:fitness best-neighbor)
                  (inc step)
                  (conj history {:step (inc step)
                                :config (:config best-neighbor)
                                :fitness (:fitness best-neighbor)}))
           ;; Local optimum reached
           (conj history {:step (inc step)
                        :config nil
                        :fitness current-fitness
                        :status :local-optimum})))))))

;; =============================================================================
;; Utility Functions
;; =============================================================================

(defn print-fitness-summary
  "Print formatted fitness summary."
  [metrics fitness config]
  (println "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
  (println "Fitness Analysis")
  (println "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
  (println (format "  N (Components):  %d" (nk/total-components config)))
  (println (format "  K (Dependencies): %d" (nk/total-interdependencies config)))
  (println (format "  Complexity Score: %.2f" (nk/complexity-score config)))
  (println)
  (println "  Metrics:")
  (println (format "    TPS:           %.2f" (:tps metrics 0.0)))
  (println (format "    Latency:       %.2f ms" (:latency metrics 0.0)))
  (when (:traceability metrics)
    (println (format "    Traceability:  %.2f" (:traceability metrics))))
  (println)
  (println "  Fitness Scores:")
  (println (format "    TPS Fitness:       %.3f" (compute-fitness-tps (:tps metrics 1.0) config)))
  (println (format "    Latency Fitness:   %.3f" (compute-fitness-latency (:latency metrics 100.0) config)))
  (println (format "    Combined Fitness:  %.3f" fitness))
  (println "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"))

(defn export-fitness-data
  "Export fitness data to CSV format for analysis."
  [fitness-data filename]
  (let [header "n,k,fitness,landscape_type\n"
        rows (map (fn [d]
                    (format "%d,%d,%.6f,%s\n"
                            (:n d)
                            (:k d)
                            (:fitness d)
                            (name (:landscape-type d :unknown))))
                  fitness-data)]
    (spit filename (str header (apply str rows)))))

(comment
  ;; Example Usage

  ;; Create a test NK config
  (def config (nk/preset-config :small-smooth))

  ;; Calculate individual fitness scores
  (compute-fitness-tps 467.94 config)
  ;; => 0.999 (near-optimal)

  (compute-fitness-latency 1.89 config)
  ;; => 0.981 (near-optimal)

  ;; Combined fitness
  (def metrics {:tps 467.94 :latency 1.89 :traceability 0.8})
  (combined-fitness metrics config)
  ;; => 0.85 (high fitness)

  ;; Landscape analysis
  (def fitness-values [0.2 0.3 0.5 0.7 0.6 0.4 0.3 0.5 0.8 0.7])
  (landscape-ruggedness fitness-values)
  ;; => 0.65 (moderately smooth)

  (count-local-optima fitness-values)
  ;; => {:count 2, :positions [3 8], :type :multi-optima}

  ;; Generate theoretical fitness surface
  (def surface (theoretical-fitness-surface [5 10 20 50] [0 1 2 3 5 10]))
  (export-fitness-data surface "fitness_surface.csv")

  ;; Full landscape characterization
  (def fitness-map {[:N5 :K0] 0.3 [:N10 :K2] 0.5 [:N20 :K5] 0.4})
  (characterize-landscape fitness-map config)
  )
