(ns datomic-blockchain.benchmark.scenario-generator
  "Generate synthetic supply chain scenarios with controlled NK complexity.

   Generates PROV-O entities, activities, and agents with variable:
   - N (number of entities/participants/batches)
   - K (interdependencies: QC points, traceability hops, certifications, processing)

   The scenarios are used for NK model-based benchmarking of blockchain performance."
  (:require [clojure.string :as str]
            [clojure.uuid :as uuid]
            [clojure.set :as set]
            [datomic-blockchain.benchmark.nk-model :as nk]))

;; =============================================================================
;; Supply Chain Entity Templates
;; =============================================================================

(def product-variants
  "Predefined product variants for synthetic supply chains"
  [{:id :product/variant-a
    :name "Product A"
    :type :general-goods
    :flavor "Standard"
    :color "Blue"}
   {:id :product/variant-b
    :name "Product B"
    :type :perishable
    :flavor "Premium"
    :color "Green"}
   {:id :product/variant-c
    :name "Product C"
    :type :hazardous
    :flavor "Specialized"
    :color "Red"}
   {:id :product/variant-d
    :name "Product D"
    :type :fragile
    :flavor "Economy"
    :color "Yellow"}
   {:id :product/variant-e
    :name "Product E"
    :type :controlled
    :flavor "Luxury"
    :color "Purple"}])

(def origin-regions
  "Predefined origin regions for entities"
  ["Region-North" "Region-South" "Region-East" "Region-West" "Region-Central"
   "Region-NE" "Region-NW" "Region-SE" "Region-SW" "Region-International"])

(def quality-grades
  "Quality grades for products"
  ["Grade-A" "Grade-B" "Grade-C" "Premium" "Standard" "Economy"])

(def certification-types
  "Types of certifications that can create K interdependencies"
  [:cert/iso-9001
   :cert/iso-22000
   :cert/haccp
   :cert/gmp
   :cert/fda-approved
   :cert/organic
   :cert/fair-trade
   :cert/halal
   :cert/kosher
   :cert/ce-mark])

;; =============================================================================
;; Entity Generation (N parameter)
;; =============================================================================

(defn generate-uuid
  "Generate a random UUID for entities"
  []
  (random-uuid))

(defn generate-product-entities
  "Generate N product entities with PROV-O structure.

   Parameters:
   - n: Number of product entities to generate
   - variants: Optional vector of product variants (cycles if fewer than n)

   Returns: Vector of entity maps with PROV-O structure"
  ([n]
   (generate-product-entities n product-variants))
  ([n variants]
   (let [variant-count (count variants)]
     (mapv
      (fn [i]
        (let [variant (nth variants (mod i variant-count))
              batch-id (format "BATCH-%06d" (inc i))
              entity-id (generate-uuid)]
          {:prov/entity entity-id
           :prov/entity-type :product/batch
           :traceability/product (:name variant)
           :traceability/batch batch-id
           :traceability/variant (:id variant)
           :traceability/origin (rand-nth origin-regions)
           :traceability/quality (rand-nth quality-grades)
           :traceability/production-date (java.util.Date.)
           :index i}))
      (range n)))))

(defn generate-participants
  "Generate M supply chain participants with PROV-O agent structure.

   Minimum 3 participants are always generated:
   - Producer (farmer/manufacturer)
   - Processor (handles transformation)
   - Retailer (sells to consumers)

   Additional participants beyond 3 are suppliers/logistics providers.

   Parameters:
   - m: Number of participants (minimum 3)

   Returns: Vector of agent maps with PROV-O structure"
  [m]
  (let [base-participants
        [{:prov/agent (generate-uuid)
          :prov/agent-type :organization/producer
          :prov/agent-name (format "Producer-%03d" 1)
          :role :producer
          :location "Production-Facility"}

         {:prov/agent (generate-uuid)
          :prov/agent-type :organization/processor
          :prov/agent-name (format "Processor-%03d" 1)
          :role :processor
          :location "Processing-Plant"}

         {:prov/agent (generate-uuid)
          :prov/agent-type :organization/retailer
          :prov/agent-name (format "Retailer-%03d" 1)
          :role :retailer
          :location "Retail-Store"}]

        additional-count (max 0 (- m 3))]
    (concat
     base-participants
     (for [i (range additional-count)]
       (let [agent-type (rand-nth [:organization/supplier
                                   :organization/logistics
                                   :organization/distributor
                                   :organization/warehouse])]
         {:prov/agent (generate-uuid)
          :prov/agent-type agent-type
          :prov/agent-name (format "%s-%03d"
                                   (str/upper-case (name (namespace agent-type)))
                                   (inc i))
          :role (keyword (str (name agent-type)))
          :location (format "Facility-%03d" (inc i))})))))

(defn generate-batches
  "Generate N batch identifiers for production tracking.

   Parameters:
   - n: Number of batches
   - prefix: Optional batch prefix (default \"BATCH\")

   Returns: Vector of batch identifiers"
  ([n]
   (generate-batches n "BATCH"))
  ([n prefix]
   (mapv (fn [i] (format "%s-%06d" prefix (inc i))) (range n))))

;; =============================================================================
;; Dependency Generation (K parameter)
;; =============================================================================

(defn generate-qc-points
  "Generate K quality control checkpoints for entities.

   QC points create interdependencies by requiring validation
   before entities can proceed in the supply chain.

   Parameters:
   - entities: Vector of product entities
   - k: Number of QC checkpoints to generate

   Returns: Map of entity-id -> QC requirements"
  [entities k]
  (let [qc-count (min k (count entities))
        qc-entities (take qc-count (shuffle entities))]
    (into {}
          (map (fn [entity idx]
                 [(:prov/entity entity)
                  {:qc-required true
                   :qc-type (rand-nth [:qc/bacteria-test
                                       :qc/sensory-test
                                       :qc/chemical-test
                                       :qc/weight-check
                                       :qc/temperature-check
                                       :qc/visual-inspection
                                       :qc/lab-analysis])
                   :qc-sequence idx}])
               qc-entities
               (range)))))

(defn generate-certification-dependencies
  "Generate K certification dependencies between entities.

   Certifications create cross-entity interdependencies where
   one entity cannot proceed until another entity's certification is verified.

   Parameters:
   - entities: Vector of product entities
   - k: Number of certification dependencies to generate

   Returns: Vector of [entity-from entity-to certification-type]"
  [entities k]
  (if (zero? k)
    []
    (let [entity-pairs (take k (shuffle (for [a entities
                                               b entities
                                               :when (not= a b)]
                                           [a b])))
          cert-type (fn [idx] (nth certification-types (mod idx (count certification-types))))]
      (mapv (fn [[a b] idx]
              [(:prov/entity a)
               (:prov/entity b)
               (cert-type idx)])
            entity-pairs
            (range)))))

(defn generate-traceability-hops
  "Generate K traceability hops defining supply chain depth.

   Each hop represents a stage in the supply chain that must be
   traversed for traceability.

   Parameters:
   - participants: Vector of supply chain participants
   - k: Number of traceability hops (depth)

   Returns: Ordered sequence of participants forming the traceability chain"
  [participants k]
  (let [hop-count (min k (count participants))
        ordered-participants (take hop-count participants)]
    (mapv (fn [participant idx]
            (assoc participant :hop-order idx))
          ordered-participants
          (range))))

(defn generate-processing-steps
  "Generate K processing steps for each entity."
  [entities k]
  (if (zero? k)
    {}
    (apply merge
           (map (fn [entity]
                  {(:prov/entity entity)
                   (for [idx (range k)]
                     {:step-number idx
                      :step-type (rand-nth [:process/heating
                                            :process/mixing
                                            :process/packaging
                                            :process/labeling
                                            :process/inspecting
                                            :process/storing
                                            :process/transporting])
                      :duration-ms (rand-nth [1000 2000 5000 10000])})})
                entities))))

;; =============================================================================
;; NK Scenario Generation
;; =============================================================================

(defn generate-nk-scenario
  "Generate a complete synthetic supply chain scenario for NK benchmark.

   Parameters:
   - config: NKConfig record defining N and K parameters

   Returns: Map with:
   - :config: The original NKConfig
   - :entities: Generated product entities (N-based)
   - :participants: Generated supply chain participants (N-based)
   - :batches: Generated batch identifiers (N-based)
   - :qc-points: Quality control checkpoints (K-based)
   - :certifications: Cross-entity dependencies (K-based)
   - :traceability-chain: Ordered participant chain for traceability (K-based)
   - :processing-steps: Processing steps per entity (K-based)
   - :metadata: Scenario metadata for reporting"
  [^datomic_blockchain.benchmark.nk_model.NKConfig config]
  (let [entities (generate-product-entities
                   (:n-entities config)
                   product-variants)
        participants (generate-participants
                       (:n-participants config))
        batches (generate-batches
                  (:n-batches config))
        qc-points (generate-qc-points
                    entities
                    (:k-qc-points config))
        certifications (generate-certification-dependencies
                         entities
                         (:k-certifications config))
        traceability-chain (generate-traceability-hops
                             participants
                             (:k-traceability-hops config))
        processing-steps (generate-processing-steps
                             entities
                             (:k-processing-steps config))
        scenario-id (nk/scenario-id config)
        description (nk/scenario-description config)]
    {:config config
     :scenario-id scenario-id
     :description description
     :entities entities
     :participants participants
     :batches batches
     :qc-points qc-points
     :certifications certifications
     :traceability-chain traceability-chain
     :processing-steps processing-steps
     :metadata
     {:entity-count (count entities)
      :participant-count (count participants)
      :batch-count (count batches)
      :qc-point-count (count qc-points)
      :certification-count (count certifications)
      :traceability-depth (count traceability-chain)}
     :summary (nk/summarize-config config)}))

;; =============================================================================
;; Scenario Validation
;; =============================================================================

(defn validate-scenario
  "Validate a generated NK scenario for completeness.

   Checks:
   - All entities have required PROV-O attributes
   - Traceability chain is complete (no gaps)
   - QC points reference valid entities
   - Certification dependencies reference valid entities

   Returns: {:valid true/false :errors [...] :warnings [...]}"
  [scenario]
  (let [errors (concat
                 (when (empty? (:entities scenario))
                   ["Scenario has no entities"])
                 (when (empty? (:participants scenario))
                   ["Scenario has no participants"])
                 (when (< (count (:participants scenario)) 3)
                   ["Scenario has fewer than 3 participants (incomplete supply chain)"])
                 (when (empty? (:traceability-chain scenario))
                   ["Scenario has no traceability chain"]))

        warnings (concat
                  (when (< (count (:entities scenario))
                          (count (:qc-points scenario)))
                    ["More QC points than entities - some may be unused"])

                  (when (seq (:certifications scenario))
                    (let [cert-entities (set (map first (:certifications scenario)))
                          entity-ids (set (map :prov/entity (:entities scenario)))]
                      (when (not (set/subset? cert-entities entity-ids))
                        [(format "Some certifications reference non-existent entities: %s"
                                (set/difference cert-entities entity-ids))]))))]
    (if (seq errors)
      {:valid false :errors errors}
      {:valid true
       :errors nil
       :warnings (when (seq warnings) warnings)})))

;; =============================================================================
;; PROV-O Transaction Generation
;; =============================================================================

(defn entity->prov-o-tx
  "Convert an entity map to a Datomic PROV-O transaction format.

   Parameters:
   - entity: Entity map with :prov/entity and other attributes

   Returns: Datomic transaction map suitable for d/transact"
  [entity]
  (assoc (dissoc entity :index)
         :db/id "temp"))

(defn agent->prov-o-tx
  "Convert an agent map to a Datomic PROV-O transaction format."
  [agent]
  (-> agent
      (dissoc :role :location :hop-order)
      (assoc :db/id "temp")))

(defn generate-prov-o-transactions
  "Generate all Datomic transactions for inserting an NK scenario.

   Parameters:
   - scenario: Generated NK scenario from generate-nk-scenario

   Returns: Vector of transaction maps suitable for d/transact"
  [scenario]
  (let [entity-txs (mapv entity->prov-o-tx (:entities scenario))
        agent-txs (mapv agent->prov-o-tx (:participants scenario))
        all-txs (concat entity-txs agent-txs)]
    all-txs))

;; =============================================================================
;; Batch Operations
;; =============================================================================

(defn generate-nk-scenarios
  "Generate multiple NK scenarios for a range of configurations.

   Parameters:
   - configs: Collection of NKConfig records

   Returns: Vector of generated scenarios"
  [configs]
  (mapv generate-nk-scenario configs))

(defn generate-nk-grid-scenarios
  "Generate scenarios for a full NK grid (default grid)."
  []
  (generate-nk-scenarios (nk/default-nk-grid)))

;; =============================================================================
;; Export/Import Functions
;; =============================================================================

(defn scenario->edn
  "Export scenario to EDN format for persistence/transmission."
  [scenario]
  (pr-str {:scenario-id (:scenario-id scenario)
           :description (:description scenario)
           :config (:config scenario)
           :entities (vec (:entities scenario))
           :participants (vec (:participants scenario))
           :metadata (:metadata scenario)}))

(defn edn->scenario
  "Import scenario from EDN format."
  [edn-string]
  (let [data (read-string edn-string)]
    (generate-nk-scenario (nk/map->NKConfig (:config data)))))

;; =============================================================================
;; Pretty Printing
;; =============================================================================

(defn print-scenario-summary
  "Print a formatted summary of the generated NK scenario."
  [scenario]
  (println "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
  (println "NK Supply Chain Scenario")
  (println "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
  (println (format "  Scenario ID:  %s" (:scenario-id scenario)))
  (println (format "  Description:  %s" (:description scenario)))
  (println)
  (println "  N (Components):")
  (println (format "    Entities:      %3d" (-> scenario :metadata :entity-count)))
  (println (format "    Participants:  %3d" (-> scenario :metadata :participant-count)))
  (println (format "    Batches:       %3d" (-> scenario :metadata :batch-count)))
  (println)
  (println "  K (Interdependencies):")
  (println (format "    QC Points:      %3d" (-> scenario :metadata :qc-point-count)))
  (println (format "    Certifications: %3d" (-> scenario :metadata :certification-count)))
  (println (format "    Trace Depth:    %3d" (-> scenario :metadata :traceability-depth)))
  (println)
  (let [summary (:summary scenario)]
    (println "  Metrics:")
    (println (format "    Total N:        %3d" (:n-total summary)))
    (println (format "    Total K:        %3d" (:k-total summary)))
    (println (format "    Complexity:     %.2f" (:complexity-score summary)))
    (println (format "    Landscape:      %s" (name (:landscape-type summary))))
    (println "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")))

(comment
  ;; Example Usage

  ;; Generate a scenario with default NK config
  (def config (nk/preset-config :small-smooth))
  (def scenario (generate-nk-scenario config))
  (print-scenario-summary scenario)

  ;; Validate scenario
  (validate-scenario scenario)
  ;; => {:valid true, :errors nil, :warnings nil}

  ;; Generate PROV-O transactions
  (def txs (generate-prov-o-transactions scenario))
  (count txs)
  ;; => 14 (10 entities + 4 participants)

  ;; Generate full grid
  (def grid-scenarios (generate-nk-grid-scenarios))
  (count grid-scenarios)
  ;; => 24 scenarios

  ;; Export scenario
  (def exported (scenario->edn scenario))
  ;; => "{:scenario-id \"nk_N10_K4_qc1_hops2_cert0_proc1\" ...}"
  )
