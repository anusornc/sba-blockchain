(ns datomic-blockchain.query.provenance
  "Datomic queries for provenance views: the authenticated trace's
   history tuples, per-entity provenance, and supply-chain paths.

   These are plain Datomic queries — the history/provenance view
   adapters over the same PROV relations the completeness assessment
   scores."
  (:require [datomic.api :as d]
            [datomic-blockchain.query.graph :as graph]))

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

;; ============================================================================
;; Entity resolution
;; ============================================================================

(defn parse-uuid-safe
  "A java.util.UUID for uuid-shaped input, nil otherwise."
  [x]
  (when (string? x)
    (try
      (java.util.UUID/fromString x)
      (catch Exception _ nil))))

(defn entity-eid-by-uuid
  "The entity id for a PROV entity uuid, nil when absent."
  [db entity-uuid]
  (d/q '[:find ?e .
         :in $ ?id
         :where [?e :prov/entity ?id]]
       db entity-uuid))

(defn resolve-product-uuid
  "Resolve a product identifier (uuid, uuid string, or batch id) to its
   PROV entity uuid."
  [db product-id]
  (or (parse-uuid-safe product-id)
      (when (uuid? product-id) product-id)
      (d/q '[:find ?pid .
             :in $ ?batch
             :where
             [?e :traceability/batch ?batch]
             [?e :prov/entity ?pid]]
           db
           product-id)))

(defn product-eid-by-qr-or-batch
  "The entity id for a product by QR code or batch id, nil when absent."
  [db qr-code batch-id]
  (cond
    qr-code
    (d/q '[:find ?e .
           :in $ ?qr
           :where [?e :traceability/qr-code ?qr]]
         db qr-code)

    batch-id
    (d/q '[:find ?e .
           :in $ ?batch
           :where [?e :traceability/batch ?batch]]
         db batch-id)

    :else nil))

(defn timeline-eids
  "Entity ids for a timeline: the entity itself plus every ancestor and
   descendant."
  [db entity-uuid]
  (let [self (entity-eid-by-uuid db entity-uuid)
        ancestors (or (graph/get-ancestors db entity-uuid) #{})
        descendants (or (graph/get-descendants db entity-uuid) #{})]
    (cond-> (concat ancestors descendants)
      self (conj self))))
