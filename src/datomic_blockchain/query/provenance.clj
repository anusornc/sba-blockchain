(ns datomic-blockchain.query.provenance
  "Datomic queries for provenance views: the authenticated trace's
   history tuples, per-entity provenance, and supply-chain paths.

   These are plain Datomic queries — the history/provenance view
   adapters over the same PROV relations the completeness assessment
   scores."
  (:require [datomic.api :as d]))

;; ============================================================================
;; Predefined Queries for Common Patterns
;; ============================================================================

(defn query-provenance
  "Query: Show complete provenance of an entity
  SPARQL equivalent:
    SELECT ?entity ?activity ?agent ?time
    WHERE {
      ?entity a prov:Entity .
      ?entity prov:wasGeneratedBy ?activity .
      ?activity prov:startedAtTime ?time .
      ?activity prov:wasAssociatedWith ?agent .
    }"
  [db entity-id]
  (d/q '[:find ?entity ?activity ?agent ?time
         :in $ ?entity-id
         :where
         [?entity :prov/entity ?entity-id]
         [?entity :prov/wasGeneratedBy ?activity-id]
         [?activity :prov/activity ?activity-id]
         [?activity :prov/startedAtTime ?time]
         [?activity :prov/wasAssociatedWith ?agent-id]
         [?agent :prov/agent ?agent-id]]
       db
       entity-id))

;; ============================================================================
;; Supply Chain Specific Queries
;; =============================================================================

(defn- product-history-generated
  [db product-id]
  (d/q '[:find ?entity ?activity ?activity-type ?agent-name ?time ?location
         :in $ ?product-id
         :where
         [?entity :prov/entity ?product-id]
         [?entity :prov/wasGeneratedBy ?activity-id]
         [?activity :prov/activity ?activity-id]
         [?activity :prov/activity-type ?activity-type]
         [?activity :prov/startedAtTime ?time]
         [(get-else $ ?activity :traceability/location "unknown") ?location]
         [?activity :prov/wasAssociatedWith ?agent-id]
         [?agent :prov/agent ?agent-id]
         [?agent :prov/agent-name ?agent-name]]
       db
       product-id))

(defn- product-history-used
  [db product-id]
  (d/q '[:find ?entity ?activity ?activity-type ?agent-name ?time ?location
         :in $ ?product-id
         :where
         [?entity :prov/entity ?product-id]
         [?activity :prov/used ?product-id]
         [?activity :prov/activity-type ?activity-type]
         [?activity :prov/startedAtTime ?time]
         [(get-else $ ?activity :traceability/location "unknown") ?location]
         [?activity :prov/wasAssociatedWith ?agent-id]
         [?agent :prov/agent ?agent-id]
         [?agent :prov/agent-name ?agent-name]]
       db
       product-id))

(defn query-product-history
  "Query: Complete history of a product through supply chain
  Returns generated-by and used events as
  [entity-eid activity-eid activity-type agent-name time location] tuples."
  [db product-id]
  (set (concat (product-history-generated db product-id)
               (product-history-used db product-id))))

(defn query-supply-chain-path
  "Query: Get full supply chain path for a product
  From producer to consumer"
  [db product-id]
  (d/q '[:find [?step ?entity ?activity ?agent ?location ?time]
         :in $ ?product-id
         :where
         [?entity :prov/entity ?product-id]
         [?entity :prov/wasGeneratedBy ?activity]
         [?activity :prov/wasAssociatedWith ?agent]
         [?activity :prov/startedAtTime ?time]
         [(get-else $ ?activity :traceability/location "unknown") ?location]]
       db
       product-id))
