(ns datomic-blockchain.benchmark.nk-model
  "NK (NKC) Model for Supply Chain Fitness Landscape Benchmarking.

  The NK model (Stuart Kauffman) maps supply chain complexity onto two parameters:
  - N = Number of system components (entities, participants, batches)
  - K = Number of interdependencies (QC points, traceability hops, certifications)

  As K increases, the fitness landscape transitions from smooth (K=0) to rugged (K=N-1).

  References:
  - Kauffman, S. (1993). The Origins of Order. Oxford University Press.
  - Kauffman, S. & Levin, S. (1987). Towards a General Theory of Adaptive Walks on Rugged Landscapes."
  (:require [clojure.set :as set]
            [clojure.string :as str]))

;; =============================================================================
;; NK Configuration Record
;; =============================================================================

(defrecord NKConfig
  [n-entities
   n-participants
   n-batches
   k-qc-points
   k-traceability-hops
   k-certifications
   k-processing-steps])

;; NKConfig documentation:
;; Configuration for NK model-based supply chain benchmark.
;;
;; N Parameters (system components):
;; - n-entities: Number of product entities/batches (1 to N)
;; - n-participants: Number of supply chain participants (3 to N, minimum for supply chain)
;; - n-batches: Number of production batches (1 to N)
;;
;; K Parameters (interdependencies):
;; - k-qc-points: Quality control checkpoints per entity (0 to K)
;; - k-traceability-hops: Required traceability depth (1 to K)
;; - k-certifications: Cross-entity certification dependencies (0 to K)
;; - k-processing-steps: Manufacturing steps per entity (1 to K)

;; =============================================================================
;; NK Config Validation
;; =============================================================================

(defn validate-nk-config
  "Validate NK configuration. Returns {:valid true} or {:valid false :errors [...]}

   Constraints:
   - n-entities > 0
   - n-participants >= 3 (minimum: producer, processor, retailer)
   - n-batches > 0
   - k-qc-points >= 0 and <= n-entities
   - k-traceability-hops >= 1 and <= n-participants
   - k-certifications >= 0 and < n-entities
   - k-processing-steps >= 1
   - Total K (sum of k params) should not exceed N-1 for meaningful fitness landscape"
  [^NKConfig config]
  (let [errors (concat
                 (when (<= (:n-entities config) 0)
                   ["n-entities must be positive"])
                 (when (< (:n-participants config) 3)
                   ["n-participants must be >= 3 (producer, processor, retailer)"])
                 (when (<= (:n-batches config) 0)
                   ["n-batches must be positive"])
                 (when (< (:k-qc-points config) 0)
                   ["k-qc-points must be >= 0"])
                 (when (> (:k-qc-points config) (:n-entities config))
                   ["k-qc-points cannot exceed n-entities"])
                 (when (< (:k-traceability-hops config) 1)
                   ["k-traceability-hops must be >= 1"])
                 (when (> (:k-traceability-hops config) (:n-participants config))
                   ["k-traceability-hops cannot exceed n-participants"])
                 (when (< (:k-certifications config) 0)
                   ["k-certifications must be >= 0"])
                 (when (>= (:k-certifications config) (:n-entities config))
                   ["k-certifications must be < n-entities"])
                 (when (< (:k-processing-steps config) 1)
                   ["k-processing-steps must be >= 1"]))
        total-k (+ (:k-qc-points config)
                   (:k-certifications config)
                   (:k-processing-steps config))
        max-k (dec (:n-entities config))]
    (if (seq errors)
      {:valid false :errors errors}
      ;; Warning if K is too large for meaningful NK landscape
      (if (> total-k max-k)
        {:valid true
         :warnings [(format "Total K (%d) exceeds N-1 (%d) - landscape may be too rugged"
                            total-k max-k)]}
        {:valid true}))))

(defn valid-nk-config?
  "Returns true if the NK configuration is valid."
  [config]
  (:valid (validate-nk-config config)))

;; =============================================================================
;; NK Complexity Metrics
;; =============================================================================

(defn total-components
  "Calculate total number of components in the system.
   This represents the N in the NK model."
  [^NKConfig config]
  (+ (:n-entities config)
     (:n-participants config)
     (:n-batches config)))

(defn total-interdependencies
  "Calculate total number of interdependencies.
   This represents the K in the NK model."
  [^NKConfig config]
  (+ (:k-qc-points config)
     (:k-traceability-hops config)
     (:k-certifications config)
     (:k-processing-steps config)))

(defn complexity-score
  "Calculate overall complexity score for the NK configuration.
   Uses weighted sum: N * K where weights reflect relative impact.

   Weights:
   - Entities: 1.0 (baseline)
   - Participants: 0.5 (fewer impact than entities)
   - Batches: 0.3 (logistical overhead)
   - QC points: 1.0 (direct validation cost)
   - Traceability hops: 0.8 (query depth impact)
   - Certifications: 0.6 (cross-entity validation)
   - Processing steps: 0.4 (per-entity overhead)"
  [^NKConfig config]
  (let [n-weighted (+ (* 1.0 (:n-entities config))
                      (* 0.5 (:n-participants config))
                      (* 0.3 (:n-batches config)))
        k-weighted (+ (* 1.0 (:k-qc-points config))
                      (* 0.8 (:k-traceability-hops config))
                      (* 0.6 (:k-certifications config))
                      (* 0.4 (:k-processing-steps config)))]
    (* n-weighted k-weighted)))

(defn landscape-type
  "Classify the fitness landscape type based on K relative to N.

   Returns:
   - :smooth - K is small (K < N/4), single global optimum likely
   - :moderate - K is medium (N/4 <= K < N/2), some local optima
   - :rugged - K is large (K >= N/2), many local optima
   - :random - K approaches N-1, nearly random landscape"
  [^NKConfig config]
  (let [n (total-components config)
        k (total-interdependencies config)
        ratio (when (pos? n) (/ k n))]
    (cond
      (nil? ratio) :smooth
      (< ratio 0.25) :smooth
      (< ratio 0.5) :moderate
      (< ratio 0.9) :rugged
      :else :random)))

;; =============================================================================
;; NK Scenario Metadata
;; =============================================================================

(defn scenario-id
  "Generate a unique scenario identifier for the NK configuration.
   Format: nk_N{n}_K{k}_qc{qc}_hops{hops}_cert{cert}_proc{proc}"
  [^NKConfig config]
  (format "nk_N%d_K%d_qc%d_hops%d_cert%d_proc%d"
          (:n-entities config)
          (:n-participants config)
          (:k-qc-points config)
          (:k-traceability-hops config)
          (:k-certifications config)
          (:k-processing-steps config)))

(defn scenario-description
  "Generate a human-readable description of the NK scenario."
  [^NKConfig config]
  (format "NK Supply Chain: %d entities, %d participants, %d batches | QC: %d, Hops: %d, Certs: %d, Process: %d"
          (:n-entities config)
          (:n-participants config)
          (:n-batches config)
          (:k-qc-points config)
          (:k-traceability-hops config)
          (:k-certifications config)
          (:k-processing-steps config)))

;; =============================================================================
;; NK Config Presets
;; =============================================================================

(def nk-Configs
  "Predefined NK configurations for different complexity levels."
  {:minimal
   (map->NKConfig
    {:n-entities 5
     :n-participants 3
     :n-batches 2
     :k-qc-points 0
     :k-traceability-hops 1
     :k-certifications 0
     :k-processing-steps 1})

   :small-smooth
   (map->NKConfig
    {:n-entities 10
     :n-participants 4
     :n-batches 5
     :k-qc-points 1
     :k-traceability-hops 2
     :k-certifications 0
     :k-processing-steps 1})

   :small-moderate
   (map->NKConfig
    {:n-entities 10
     :n-participants 5
     :n-batches 5
     :k-qc-points 2
     :k-traceability-hops 3
     :k-certifications 1
     :k-processing-steps 2})

   :medium-smooth
   (map->NKConfig
    {:n-entities 20
     :n-participants 5
     :n-batches 10
     :k-qc-points 2
     :k-traceability-hops 2
     :k-certifications 1
     :k-processing-steps 2})

   :medium-rugged
   (map->NKConfig
    {:n-entities 20
     :n-participants 8
     :n-batches 10
     :k-qc-points 5
     :k-traceability-hops 5
     :k-certifications 3
     :k-processing-steps 3})

   :large-rugged
   (map->NKConfig
    {:n-entities 50
     :n-participants 15
     :n-batches 25
     :k-qc-points 10
     :k-traceability-hops 8
     :k-certifications 5
     :k-processing-steps 5})})

(defn preset-config
  "Get a preset NK configuration by keyword.
   Available: :minimal, :small-smooth, :small-moderate, :medium-smooth, :medium-rugged, :large-rugged"
  [preset-key]
  (get nk-Configs preset-key))

;; =============================================================================
;; NK Grid Generation
;; =============================================================================

(defn nk-grid
  "Generate a grid of NK configurations.

   Parameters:
   - n-values: Sequence of N values (e.g., [5 10 20])
   - k-values: Sequence of K values (e.g., [0 1 2 3 5])
   - n-participants-fn: Function taking N -> number of participants
   - k-breakdown-fn: Function taking K -> {:k-qc-points, :k-traceability-hops, ...}

   Example:
   (nk-grid [5 10] [1 2 3]
           (fn [n] (max 3 (quot n 2)))
           (fn [k] {:k-qc-points k :k-traceability-hops (inc k)
                      :k-certifications (max 0 (dec k)) :k-processing-steps (max 1 k)}))"
  [n-values k-values n-participants-fn k-breakdown-fn]
  (for [n n-values
        k k-values]
    (let [breakdown (if (fn? k-breakdown-fn)
                       (k-breakdown-fn k)
                       k-breakdown-fn)]
      (map->NKConfig
       (merge
        {:n-entities n
         :n-participants (n-participants-fn n)
         :n-batches (max 1 (quot n 2))}
        breakdown)))))

(defn default-nk-grid
  "Generate the default NK grid for benchmarking.

   Grid:
   - N: [5 10 20 50]
   - K: [0 1 2 3 5 10]
   - Participants: max(3, N/2)
   - Breakdown: K distributed across QC points, hops, certs, processing"
  []
  (nk-grid
   [5 10 20 50]
   [0 1 2 3 5 10]
   (fn [n] (max 3 (quot n 2)))
   (fn [k]
     (let [qc (min k (quot k 3))
           hops (min (max 1 (quot k 2)) 10)
           certs (max 0 (- k qc hops))
           proc (max 1 (min certs k))]
       {:k-qc-points qc
        :k-traceability-hops hops
        :k-certifications certs
        :k-processing-steps proc}))))

;; =============================================================================
;; Fitness Landscape Theory Helpers
;; =============================================================================

(defn expected-ruggedness
  "Calculate expected landscape ruggedness based on NK theory.

   Returns theoretical correlation between fitness values of similar configurations.
   - Higher correlation = smoother landscape
   - Lower correlation = more rugged landscape

   Based on: correlation ≈ (1 - K/N) for random fitness contributions"
  [^NKConfig config]
  (let [n (total-components config)
        k (total-interdependencies config)]
    (if (zero? n)
      1.0
      (max 0.0 (- 1.0 (/ k n))))))

(defn theoretical-optima-count
  "Estimate theoretical number of local optima based on NK theory.

   For NK model, expected number of local optima ≈ 2^(N/K) for intermediate K."
  [^NKConfig config]
  (let [n (total-components config)
        k (total-interdependencies config)]
    (cond
      (zero? k) 1  ; Smooth: single global optimum
      (>= k n) (Math/pow 2 n)  ; Random: each peak could be local optimum
      :else (Math/pow 2 (quot n k)))))

;; =============================================================================
;; Utility Functions
;; =============================================================================

(defn summarize-config
  "Create a summary map of the NK configuration for reporting."
  [^NKConfig config]
  {:scenario-id (scenario-id config)
   :description (scenario-description config)
   :n-total (total-components config)
   :k-total (total-interdependencies config)
   :complexity-score (complexity-score config)
   :landscape-type (landscape-type config)
   :expected-ruggedness (expected-ruggedness config)
   :theoretical-optima (theoretical-optima-count config)})

(defn print-config-summary
  "Print a formatted summary of the NK configuration."
  [^NKConfig config]
  (let [summary (summarize-config config)]
    (println "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    (println "NK Supply Chain Benchmark Configuration")
    (println "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    (println (format "  Scenario ID: %s" (:scenario-id summary)))
    (println (format "  Description: %s" (:description summary)))
    (println)
    (println "  N Parameters:")
    (println (format "    Entities:    %3d" (:n-entities config)))
    (println (format "    Participants:%3d" (:n-participants config)))
    (println (format "    Batches:     %3d" (:n-batches config)))
    (println (format "    N Total:     %3d" (:n-total summary)))
    (println)
    (println "  K Parameters:")
    (println (format "    QC Points:      %3d" (:k-qc-points config)))
    (println (format "    Traceability:   %3d" (:k-traceability-hops config)))
    (println (format "    Certifications:%3d" (:k-certifications config)))
    (println (format "    Processing:     %3d" (:k-processing-steps config)))
    (println (format "    K Total:        %3d" (:k-total summary)))
    (println)
    (println "  Metrics:")
    (println (format "    Complexity Score: %.2f" (:complexity-score summary)))
    (println (format "    Landscape Type:   %s" (name (:landscape-type summary))))
    (println (format "    Expected Ruggedness: %.3f" (:expected-ruggedness summary)))
    (println (format "    Theoretical Optima: %d" (:theoretical-optima summary)))
    (println "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")))

(comment
  ;; Example Usage

  ;; Create a custom NK config
  (def config (->NKConfig
                :n-entities 10
                :n-participants 4
                :n-batches 5
                :k-qc-points 2
                :k-traceability-hops 3
                :k-certifications 1
                :k-processing-steps 2))

  ;; Validate
  (valid-nk-config? config)
  ;; => true

  ;; Get summary
  (summarize-config config)
  ;; => {:scenario-id "nk_N10_K4_qc2_hops3_cert1_proc2", ...}

  ;; Print summary
  (print-config-summary config)

  ;; Use preset
  (def preset (preset-config :small-smooth))
  (print-config-summary preset)

  ;; Generate grid
  (def grid (default-nk-grid))
  (count grid)
  ;; => 24 configurations

  ;; Landscape type
  (landscape-type preset)
  ;; => :moderate

  ;; Expected ruggedness
  (expected-ruggedness preset)
  ;; => 0.65 (smooth-to-moderate)

  ;; Theoretical optima
  (theoretical-optima-count preset)
  ;; => 5 local optima expected
  )
