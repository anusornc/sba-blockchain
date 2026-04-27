(ns datomic-blockchain.ontology.validator
  "Ontology validation system
  Validates entities against ontology constraints"
  (:require [clojure.set :as set]
            [taoensso.timbre :as log]
            [datomic.api :as d]))

;; ============================================================================
;; Validation Results
;; ============================================================================

(defrecord ValidationResult [valid? errors warnings])

(defn validation-result
  "Create a validation result"
  ([valid?]
   (validation-result valid? [] []))
  ([valid? errors]
   (validation-result valid? errors []))
  ([valid? errors warnings]
   ->ValidationResult {:valid? valid?
                       :errors errors
                       :warnings warnings}))

(defn merge-results
  "Merge multiple validation results"
  [& results]
  (let [all-valid? (every? :valid? results)
        all-errors (mapcat :errors results)
        all-warnings (mapcat :warnings results)]
    (validation-result all-valid? all-errors all-warnings)))

;; ============================================================================
;; PROV-O Validation Rules
;; ============================================================================

(def prov-entity-constraints
  "Required attributes for PROV-O Entity"
  #{:prov/entity
    :prov/entity-type})

(def prov-activity-constraints
  "Required attributes for PROV-O Activity"
  #{:prov/activity
    :prov/activity-type
    :prov/startedAtTime})

(def prov-agent-constraints
  "Required attributes for PROV-O Agent"
  #{:prov/agent
    :prov/agent-type})

;; ============================================================================
;; Entity Validation
;; ============================================================================

(defn validate-prov-entity
  "Validate that entity meets PROV-O Entity constraints"
  [entity]
  (let [entity-type (:prov/entity-type entity)
        required-attrs prov-entity-constraints
        present-attrs (set (keys entity))
        missing-attrs (set/difference required-attrs present-attrs)]
    (if (empty? missing-attrs)
      (validation-result true)
      (validation-result false
                       [(str "PROV-O Entity missing required attributes: "
                             (clojure.string/join ", " missing-attrs))]))))

(defn validate-prov-activity
  "Validate that entity meets PROV-O Activity constraints"
  [activity]
  (let [required-attrs prov-activity-constraints
        present-attrs (set (keys activity))
        missing-attrs (set/difference required-attrs present-attrs)
        errors (cond-> []
                    (not-empty missing-attrs)
                    (conj (str "PROV-O Activity missing required attributes: "
                               (clojure.string/join ", " missing-attrs))))

        ;; Time validation
        start-time (:prov/startedAtTime activity)
        end-time (:prov/endedAtTime activity)
        warnings (cond
                   (and start-time end-time
                        (.after start-time end-time))
                   [(str "Activity end time before start time")]

                   :else
                   [])]
    (validation-result (empty? errors) errors warnings)))

(defn validate-prov-agent
  "Validate that entity meets PROV-O Agent constraints"
  [agent]
  (let [required-attrs prov-agent-constraints
        present-attrs (set (keys agent))
        missing-attrs (set/difference required-attrs present-attrs)]
    (if (empty? missing-attrs)
      (validation-result true)
      (validation-result false
                       [(str "PROV-O Agent missing required attributes: "
                             (clojure.string/join ", " missing-attrs))]))))

;; ============================================================================
;; Relationship Validation
;; ============================================================================

(defn validate-entity-activity-link
  "Validate that entity-activity link is valid"
  [db entity-id activity-id]
  (let [entity (d/entity db entity-id)
        activity (d/entity db activity-id)]
    (cond
      (nil? entity)
      (validation-result false ["Entity not found"])

      (nil? activity)
      (validation-result false ["Activity not found"])

      (not (contains? entity :prov/entity))
      (validation-result false ["Referenced entity is not a PROV-O Entity"])

      (not (contains? activity :prov/activity))
      (validation-result false ["Referenced activity is not a PROV-O Activity"])

      :else
      (validation-result true))))

(defn validate-activity-agent-link
  "Validate that activity-agent link is valid"
  [db activity-id agent-id]
  (let [activity (d/entity db activity-id)
        agent (d/entity db agent-id)]
    (cond
      (nil? activity)
      (validation-result false ["Activity not found"])

      (nil? agent)
      (validation-result false ["Agent not found"])

      (not (contains? activity :prov/activity))
      (validation-result false ["Referenced activity is not a PROV-O Activity"])

      (not (contains? agent :prov/agent))
      (validation-result false ["Referenced agent is not a PROV-O Agent"])

      :else
      (validation-result true))))

(defn validate-derivation
  "Validate that derivation relationship is valid"
  [db derived-entity-id used-entity-id]
  (let [derived (d/entity db derived-entity-id)
        used (d/entity db used-entity-id)]
    (cond
      (nil? derived)
      (validation-result false ["Derived entity not found"])

      (nil? used)
      (validation-result false ["Used entity not found"])

      (not (contains? derived :prov/entity))
      (validation-result false ["Derived entity is not a PROV-O Entity"])

      (not (contains? used :prov/entity))
      (validation-result false ["Used entity is not a PROV-O Entity"])

      (= derived-entity-id used-entity-id)
      (validation-result false ["Entity cannot derive from itself"])

      :else
      (validation-result true))))

;; ============================================================================
;; Supply Chain Validation
;; ============================================================================

(defn validate-product-entity
  "Validate that product entity has required supply chain attributes"
  [entity]
  (let [entity-type (:prov/entity-type entity)]
    (if (and entity-type
             (or (= entity-type :product/batch)
                 (= entity-type :product/item)))
      (let [required #{:traceability/product}
            present (set (keys entity))
            missing (set/difference required present)]
        (if (empty? missing)
          (validation-result true)
          (validation-result false
                           [(str "Product missing required attributes: "
                                 (clojure.string/join ", " missing))])))
      (validation-result true))))

(defn validate-batch-info
  "Validate batch information is complete"
  [entity]
  (if (:traceability/batch entity)
    (let [batch (:traceability/batch entity)]
      (if (and (string? batch)
               (> (count batch) 0))
        (validation-result true)
        (validation-result false ["Batch number must be non-empty string"])))
    (validation-result true)))

(defn validate-location
  "Validate location information"
  [location]
  (cond
    (nil? location)
    (validation-result true)

    (string? location)
    (if (> (count location) 0)
      (validation-result true)
      (validation-result false ["Location cannot be empty string"]))

    (map? location)
    (let [has-lat (contains? location :lat)
          has-lng (contains? location :lng)]
      (if (and has-lat has-lng)
        (validation-result true)
        (validation-result false ["Location map must have :lat and :lng"])))

    :else
    (validation-result false ["Location must be string or map"])))

;; ============================================================================
;; Complete Entity Validation
;; ============================================================================

(defn validate-entity
  "Complete validation of entity against ontology
  Checks PROV-O constraints and domain-specific rules"
  ([db entity-id]
   (validate-entity db entity-id nil))
  ([db entity-id opts]
   (log/debug "Validating entity:" entity-id)
   (let [entity (d/entity db entity-id)]

     (cond
       (nil? entity)
       (validation-result false ["Entity not found in database"])

       ;; Check entity type
       (contains? entity :prov/entity)
       (merge-results
        (validate-prov-entity entity)
        (validate-product-entity entity)
        (validate-batch-info entity))

       ;; Check activity type
       (contains? entity :prov/activity)
       (validate-prov-activity entity)

       ;; Check agent type
       (contains? entity :prov/agent)
       (validate-prov-agent entity)

       :else
       (validation-result false ["Unknown entity type - no PROV-O attributes found"])))))

(defn validate-entity-with-relationships
  "Validate entity and all its relationships"
  [db entity-id]
  (log/info "Validating entity with relationships:" entity-id)
  (let [entity-result (validate-entity db entity-id)
        entity (d/entity db entity-id)

        ;; Validate wasGeneratedBy relationship
        gen-activity (:prov/wasGeneratedBy entity)
        gen-result (if gen-activity
                     (validate-entity-activity-link db entity-id gen-activity)
                     (validation-result true))

        ;; Validate wasAssociatedWith relationships
        agents (:prov/wasAssociatedWith entity)
        agent-results (if (coll? agents)
                        (mapv (fn [agent-id]
                                (if (contains? entity :prov/activity)
                                  (validate-activity-agent-link db entity-id agent-id)
                                  (validation-result true)))
                              agents)
                        [(validation-result true)])

        ;; Validate wasDerivedFrom relationships
        derived-from (:prov/wasDerivedFrom entity)
        derivation-results (if (coll? derived-from)
                             (mapv (fn [derived-id]
                                     (validate-derivation db entity-id derived-id))
                                   derived-from)
                             [(validation-result true)])]

    ;; Merge all results
    (merge-results entity-result
                  gen-result
                  (apply merge-results agent-results)
                  (apply merge-results derivation-results))))

;; ============================================================================
;; Batch Validation
;; ============================================================================

(defn validate-entities
  "Validate multiple entities"
  [db entity-ids]
  (log/info "Batch validating" (count entity-ids) "entities")
  (reduce (fn [acc entity-id]
            (let [result (validate-entity db entity-id)]
              (update acc :summary
                      (fn [s]
                        (update s (if (:valid? result) :valid :invalid) inc)))
              (update acc :results conj [entity-id result])))
          {:summary {:valid 0 :invalid 0}
           :results []}
          entity-ids))

(defn validate-database
  "Validate all entities in database"
  [db]
  (log/info "Validating entire database")
  (let [entities (d/q '[:find [?e] :where [?e :prov/entity]] db)
        activities (d/q '[:find [?e] :where [?e :prov/activity]] db)
        agents (d/q '[:find [?e] :where [?e :prov/agent]] db)
        all-ids (concat entities activities agents)]
    (validate-entities db all-ids)))

;; ============================================================================
;; Report Generation
;; ============================================================================

(defn format-validation-report
  "Format validation result as readable report"
  [result]
  (str
   "Validation: " (if (:valid? result) "✓ VALID" "✗ INVALID") "\n"
   (when-not (empty? (:errors result))
     (str "Errors:\n"
          (clojure.string/join "\n"
                               (map (fn [e] (str "  - " e))
                                    (:errors result)))
          "\n"))
   (when-not (empty? (:warnings result))
     (str "Warnings:\n"
          (clojure.string/join "\n"
                               (map (fn [w] (str "  - " w))
                                    (:warnings result)))
          "\n"))))

(defn print-validation-report
  "Print validation report to console"
  [entity-id result]
  (println "=== Validation Report ===")
  (println "Entity:" entity-id)
  (println (format-validation-report result))
  (when (:valid? result)
    (println "Entity is valid!"))
  (println "========================"))

;; ============================================================================
;; Development Helpers
;; ============================================================================

(comment
  ;; Development REPL usage
  (def db (dev/db))

  ;; Validate single entity
  (def result (validate-entity db some-entity-id))
  (print-validation-report some-entity-id result)

  ;; Validate with relationships
  (validate-entity-with-relationships db some-entity-id)

  ;; Batch validate
  (def entity-ids [id1 id2 id3])
  (validate-entities db entity-ids)

  ;; Validate entire database
  (validate-database db))
