(ns datomic-blockchain.benchmark.test-scenario-generator
  "Tests for NK model supply chain scenario generation.

   Tests cover:
   - Product entity generation with PROV-O structure
   - Participant generation (minimum 3)
   - Batch generation
   - QC point generation
   - Certification dependencies
   - Traceability hops
   - Processing steps
   - Complete scenario generation
   - Scenario validation
   - PROV-O transaction conversion"
  (:require [clojure.test :refer :all]
            [clojure.set :as set]
            [datomic-blockchain.benchmark.nk-model :as nk]
            [datomic-blockchain.benchmark.scenario-generator :as sg]))

;; =============================================================================
;; Product Entity Generation Tests
;; =============================================================================

(deftest generate-product-entities-count-test
  (testing "Generates correct number of product entities"
    (let [n 10
          entities (sg/generate-product-entities n)]
      (is (= n (count entities)))
      (is (vector? entities)))))

(deftest generate-product-entities-prov-o-structure-test
  (testing "Generated entities have required PROV-O attributes"
    (let [entities (sg/generate-product-entities 5)
          entity (first entities)]
      (is (contains? entity :prov/entity))
      (is (contains? entity :prov/entity-type))
      (is (= :product/batch (:prov/entity-type entity)))
      (is (contains? entity :traceability/product))
      (is (contains? entity :traceability/batch))
      (is (uuid? (:prov/entity entity))))))

(deftest generate-product-entities-with-custom-variants-test
  (testing "Custom variants are used when provided"
    (let [variants [{:id :test/a :name "Test A" :type :test}
                    {:id :test/b :name "Test B" :type :test}]
          entities (sg/generate-product-entities 4 variants)]
      (is (= 4 (count entities)))
      (is (= "Test A" (:traceability/product (nth entities 0))))
      (is (= "Test B" (:traceability/product (nth entities 1)))))))

;; =============================================================================
;; Participant Generation Tests
;; =============================================================================

(deftest generate-participants-minimum-three-test
  (testing "Always generates at least 3 participants"
    (let [participants (sg/generate-participants 1)]
      (is (= 3 (count participants)))
      (is (= :organization/producer
             (:prov/agent-type (nth participants 0))))
      (is (= :organization/processor
             (:prov/agent-type (nth participants 1))))
      (is (= :organization/retailer
             (:prov/agent-type (nth participants 2)))))))

(deftest generate-participants-exact-count-test
  (testing "Generates exact number of participants when > 3"
    (let [participants (sg/generate-participants 5)]
      (is (= 5 (count participants))))))

(deftest generate-participants-prov-o-structure-test
  (testing "Participants have required PROV-O agent attributes"
    (let [participants (sg/generate-participants 3)
          agent (first participants)]
      (is (contains? agent :prov/agent))
      (is (contains? agent :prov/agent-type))
      (is (contains? agent :prov/agent-name))
      (is (contains? agent :role))
      (is (uuid? (:prov/agent agent))))))

(deftest generate-participants-has-producer-processor-retailer-test
  (testing "Base participants include producer, processor, retailer"
    (let [participants (sg/generate-participants 10)
          agent-types (map :prov/agent-type participants)]
      (is (some #(= % :organization/producer) agent-types))
      (is (some #(= % :organization/processor) agent-types))
      (is (some #(= % :organization/retailer) agent-types)))))

;; =============================================================================
;; Batch Generation Tests
;; =============================================================================

(deftest generate-batches-count-test
  (testing "Generates correct number of batches"
    (let [n 5
          batches (sg/generate-batches n)]
      (is (= n (count batches)))
      (is (vector? batches)))))

(deftest generate-batches-format-test
  (testing "Generated batches have correct format"
    (let [batches (sg/generate-batches 3)]
      (is (.startsWith (first batches) "BATCH-"))
      (is (.startsWith (second batches) "BATCH-"))
      (is (not= (first batches) (second batches))))))

(deftest generate-batches-custom-prefix-test
  (testing "Custom prefix is used when provided"
    (let [batches (sg/generate-batches 3 "CUSTOM")]
      (is (.startsWith (first batches) "CUSTOM-")))))

;; =============================================================================
;; QC Point Generation Tests
;; =============================================================================

(deftest generate-qc-points-count-test
  (testing "Generates at most K QC points"
    (let [entities (sg/generate-product-entities 10)
          k 5
          qc-points (sg/generate-qc-points entities k)]
      (is (<= (count qc-points) k))
      (is (map? qc-points)))))

(deftest generate-qc-points-zero-k-test
  (testing "Zero K generates no QC points"
    (let [entities (sg/generate-product-entities 10)
          qc-points (sg/generate-qc-points entities 0)]
      (is (zero? (count qc-points))))))

(deftest generate-qc-points-reference-valid-entities-test
  (testing "QC points reference valid entity IDs"
    (let [entities (sg/generate-product-entities 5)
          qc-points (sg/generate-qc-points entities 3)
          entity-ids (set (map :prov/entity entities))
          qc-ids (set (keys qc-points))]
      (is (set/subset? qc-ids entity-ids)))))

;; =============================================================================
;; Certification Dependency Tests
;; =============================================================================

(deftest generate-certifications-zero-k-test
  (testing "Zero K generates no certification dependencies"
    (let [entities (sg/generate-product-entities 5)
          certs (sg/generate-certification-dependencies entities 0)]
      (is (zero? (count certs))))))

(deftest generate-certifications-count-test
  (testing "Generates K certification dependencies"
    (let [entities (sg/generate-product-entities 10)
          k 5
          certs (sg/generate-certification-dependencies entities k)]
      (is (= k (count certs)))
      (is (vector? certs)))))

(deftest generate-certifications-structure-test
  (testing "Certification dependencies have correct structure"
    (let [entities (sg/generate-product-entities 5)
          certs (sg/generate-certification-dependencies entities 2)
          cert (first certs)]
      (is (vector? cert))
      (is (= 3 (count cert)))  ; [entity-from entity-to cert-type]
      (is (keyword? (nth cert 2))))))

;; =============================================================================
;; Traceability Hops Tests
;; =============================================================================

(deftest generate-traceability-hops-count-test
  (testing "Generates at most K traceability hops"
    (let [participants (sg/generate-participants 10)
          k 5
          hops (sg/generate-traceability-hops participants k)]
      (is (<= (count hops) k)))))

(deftest generate-traceability-hops-ordered-test
  (testing "Traceability hops maintain order"
    (let [participants (sg/generate-participants 5)
          hops (sg/generate-traceability-hops participants 3)]
      (is (= 3 (count hops)))
      (is (= 0 (:hop-order (first hops))))
      (is (= 1 (:hop-order (second hops)))))))

;; =============================================================================
;; Processing Steps Tests
;; =============================================================================

(deftest generate-processing-steps-zero-k-test
  (testing "Zero K generates no processing steps"
    (let [entities (sg/generate-product-entities 5)
          steps (sg/generate-processing-steps entities 0)]
      (is (zero? (count steps))))))

(deftest generate-processing-steps-count-test
  (testing "Each entity gets K processing steps"
    (let [entities (sg/generate-product-entities 3)
          k 2
          steps (sg/generate-processing-steps entities k)]
      (is (= 3 (count steps)))  ; One entry per entity
      (is (= k (count (get steps (-> entities first :prov/entity)))))))

(deftest generate-processing-steps-structure-test
  (testing "Processing steps have correct structure"
    (let [entities (sg/generate-product-entities 2)
          steps (sg/generate-processing-steps entities 1)
          entity-id (-> entities first :prov/entity)
          entity-steps (get steps entity-id)
          step (first entity-steps)]
      (is (contains? step :step-number))
      (is (contains? step :step-type))
      (is (contains? step :duration-ms)))))

;; =============================================================================
;; Complete Scenario Generation Tests
;; =============================================================================

(deftest generate-nk-scenario-structure-test
  (testing "Generated scenario has all expected keys"
    (let [config (nk/preset-config :small-smooth)
          scenario (sg/generate-nk-scenario config)]
      (is (contains? scenario :config))
      (is (contains? scenario :scenario-id))
      (is (contains? scenario :description))
      (is (contains? scenario :entities))
      (is (contains? scenario :participants))
      (is (contains? scenario :batches))
      (is (contains? scenario :qc-points))
      (is (contains? scenario :certifications))
      (is (contains? scenario :traceability-chain))
      (is (contains? scenario :processing-steps))
      (is (contains? scenario :metadata))
      (is (contains? scenario :summary)))))

(deftest generate-nk-scenario-counts-match-config-test
  (testing "Scenario counts match NK config"
    (let [config (nk/preset-config :small-smooth)
          scenario (sg/generate-nk-scenario config)]
      (is (= (:n-entities config)
             (-> scenario :metadata :entity-count)))
      (is (= (:n-participants config)
             (-> scenario :metadata :participant-count)))
      (is (= (:n-batches config)
             (-> scenario :metadata :batch-count))))))

(deftest generate-nk-scenario-metadata-test
  (testing "Scenario metadata contains all counts"
    (let [config (nk/preset-config :small-smooth)
          scenario (sg/generate-nk-scenario config)
          metadata (:metadata scenario)]
      (is (contains? metadata :entity-count))
      (is (contains? metadata :participant-count))
      (is (contains? metadata :batch-count))
      (is (contains? metadata :qc-point-count))
      (is (contains? metadata :certification-count))
      (is (contains? metadata :traceability-depth)))))

;; =============================================================================
;; Scenario Validation Tests
;; =============================================================================

(deftest validate-valid-scenario-test
  (testing "Valid generated scenario passes validation"
    (let [config (nk/preset-config :small-smooth)
          scenario (sg/generate-nk-scenario config)
          result (sg/validate-scenario scenario)]
      (is (true? (:valid result)))
      (is (nil? (:errors result))))))

(deftest validate-empty-entities-fails-test
  (testing "Scenario with no entities fails validation"
    (let [scenario {:entities []
                     :participants (sg/generate-participants 3)
                     :traceability-chain []}
          result (sg/validate-scenario scenario)]
      (is (false? (:valid result)))
      (is (some #(re-find #"entities" %) (:errors result)))))))

(deftest validate-empty-participants-fails-test
  (testing "Scenario with no participants fails validation"
    (let [scenario {:entities (sg/generate-product-entities 1)
                     :participants []
                     :traceability-chain []}
          result (sg/validate-scenario scenario)]
      (is (false? (:valid result)))
      (is (some #(re-find #"participants" %) (:errors result))))))

(deftest validate-fewer-than-three-participants-fails-test
  (testing "Scenario with fewer than 3 participants fails validation"
    (let [scenario {:entities (sg/generate-product-entities 1)
                     :participants [(first (sg/generate-participants 1))]
                     :traceability-chain []}
          result (sg/validate-scenario scenario)]
      (is (false? (:valid result)))
      (is (some #(re-find #"3 participants" %) (:errors result))))))

;; =============================================================================
;; PROV-O Transaction Conversion Tests
;; =============================================================================

(deftest entity-to-prov-o-tx-test
  (testing "Entity is converted to valid Datomic transaction format"
    (let [entities (sg/generate-product-entities 1)
          entity (first entities)
          tx (sg/entity->prov-o-tx entity)]
      (is (contains? tx :db/id))
      (is (= "temp" (:db/id tx)))
      (is (contains? tx :prov/entity))
      (is (not (contains? tx :index))))))  ; :index should be removed

(deftest agent-to-prov-o-tx-test
  (testing "Agent is converted to valid Datomic transaction format"
    (let [participants (sg/generate-participants 3)
          agent (first participants)
          tx (sg/agent->prov-o-tx agent)]
      (is (contains? tx :db/id))
      (is (= "temp" (:db/id tx)))
      (is (contains? tx :prov/agent))
      (is (not (contains? tx :role)))  ; :role should be removed
      (is (not (contains? tx :location))))))  ; :location should be removed

(deftest generate-prov-o-transactions-test
  (testing "All scenario data is converted to transactions"
    (let [config (nk/preset-config :small-smooth)
          scenario (sg/generate-nk-scenario config)
          txs (sg/generate-prov-o-transactions scenario)]
      (is (seq? txs))
      (is (pos? (count txs)))
      (is (= (+ (-> scenario :metadata :entity-count)
                 (-> scenario :metadata :participant-count))
             (count txs))))))

;; =============================================================================
;; Batch Operations Tests
;; =============================================================================

(deftest generate-nk-scenarios-test
  (testing "Multiple scenarios can be generated from configs"
    (let [configs [(nk/preset-config :minimal)
                   (nk/preset-config :small-smooth)]
          scenarios (sg/generate-nk-scenarios configs)]
      (is (= 2 (count scenarios)))
      (is (every? #(-> % :metadata :entity-count pos?) scenarios)))))

(deftest generate-nk-grid-scenarios-count-test
  (testing "Default grid generates expected number of scenarios"
    (let [scenarios (sg/generate-nk-grid-scenarios)]
      (is (= 24 (count scenarios))))))  ; 4 N × 6 K from default grid

;; =============================================================================
;; Export/Import Tests
;; =============================================================================

(deftest scenario-to-edn-test
  (testing "Scenario can be exported to EDN format"
    (let [config (nk/preset-config :small-smooth)
          scenario (sg/generate-nk-scenario config)
          edn (sg/scenario->edn scenario)]
      (is (string? edn))
      (is (.contains edn ":scenario-id"))
      (is (.contains edn ":config")))))

(deftest scenario-to-edn-roundtrip-test
  (testing "EDN export contains all required data"
    (let [config (nk/preset-config :small-smooth)
          scenario (sg/generate-nk-scenario config)
          edn (sg/scenario->edn scenario)
          data (read-string edn)]
      (is (contains? data :scenario-id))
      (is (contains? data :description))
      (is (contains? data :config))
      (is (contains? data :entities))))
)
