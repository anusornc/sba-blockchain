(ns datomic-blockchain.query.sparql
  "SPARQL-like query interface for Datomic knowledge graph
  Provides familiar SPARQL patterns over Datomic database"
  (:require [clojure.string :as str]
            [taoensso.timbre :as log]
            [datomic.api :as d])
  (:import [java.util Date])
  (:refer-clojure :exclude [find distinct]))

;; ============================================================================
;; SPARQL-like Query Builder
;; ============================================================================

(defrecord SPARQLQuery [find where where-optional order limit offset distinct])

(defn query
  "Create a new SPARQL-like query"
  []
  ->SPARQLQuery {:find []
                 :where []
                 :where-optional []
                 :order []
                 :limit nil
                 :offset nil
                 :distinct false})

(defn find
  "SELECT clause - specify variables to find"
  ([q vars]
   (assoc q :find (if (coll? vars) vars [vars])))
  ([q] q))

(defn where
  "WHERE clause - add triple patterns"
  [q & patterns]
  (update q :where concat (vec patterns)))

(defn optional
  "OPTIONAL clause - add optional patterns"
  [q & patterns]
  (update q :where-optional concat (vec patterns)))

(defn order-by
  "ORDER BY clause - sort results"
  [q & vars]
  (assoc q :order vars))

(defn limit
  "LIMIT clause - restrict number of results"
  [q n]
  (assoc q :limit n))

(defn offset
  "OFFSET clause - skip results"
  [q n]
  (assoc q :offset n))

(defn distinct
  "DISTINCT modifier - return unique results"
  [q]
  (assoc q :distinct true))

;; ============================================================================
;; Triple Pattern Matching
;; ============================================================================

(defn triple-pattern
  "Create a triple pattern for WHERE clause
  Subject, predicate, object can be:
  - Keywords (variables): ?subject ?predicate ?object
  - Strings (URIs/literals): 'http://example.org/entity'
  - Keywords (attributes): :prov/entity"
  [subject predicate object]
  {:subject subject
   :predicate predicate
   :object object})

(defn ?var
  "Create a variable (like SPARQL ?variable)"
  [name]
  (keyword (str "?" (name name))))

;; ============================================================================
;; Query Compilation to Datomic
;; ============================================================================

(defn compile-triple-pattern
  "Convert triple pattern to Datomic where clause"
  [pattern]
  (let [s (:subject pattern)
        p (:predicate pattern)
        o (:object pattern)

        ;; Variable detection
        s-var? (and (keyword? s) (str/starts-with? (name s) "?"))
        p-var? (and (keyword? p) (str/starts-with? (name p) "?"))
        o-var? (and (keyword? o) (str/starts-with? (name o) "?"))

        ;; Entity ID variable
        e-var (or (when s-var? s) :?e)]

    ;; Build Datomic where clauses
    (cond
      ;; All variables (invalid)
      (and s-var? p-var? o-var?)
      (throw (ex-info "Invalid triple pattern: all variables" pattern))

      ;; Subject variable, predicate and object fixed
      s-var?
      (cond
        o-var?
        [e-var p :?o]  ;; ?s :pred ?o

        :else
        [e-var p o])  ;; ?s :pred "value"

      ;; Subject fixed, predicate and/or object variable
      p-var?
      [:?s p :?o]

      o-var?
      [s p :?o]

      ;; All fixed (not a query)
      :else
      [s p o])))

(defn compile-query
  "Convert SPARQL-like query to Datomic query"
  [q db]
  (let [find-vars (:find q)
        where-patterns (:where q)
        optional-patterns (:where-optional q)

        ;; Compile WHERE clauses
        compiled-where (mapcat compile-triple-pattern where-patterns)

        ;; Build Datomic query
        datomic-query (cond-> {:find (vec find-vars)
                              :where compiled-where}
                         (:order q) (assoc :order (:order q))
                         (:limit q) (assoc :limit (:limit q))
                         (:offset q) (assoc :offset (:offset q))
                         (:distinct q) (assoc :limit (:limit q)))]


    datomic-query))

;; ============================================================================
;; Query Execution
;; ============================================================================

(defn execute
  "Execute SPARQL-like query against database"
  [q db]
  (let [compiled (compile-query q db)
        results (d/q compiled db)]
    (log/debug "Query returned" (count results) "results")
    results))

(defn execute-query
  "Convenience: execute query directly"
  ([conn find-vars where-clauses]
   (execute-query conn find-vars where-clauses nil))
  ([conn find-vars where-clauses opts]
   (let [db (d/db conn)
         q-base (-> (query)
                    (find find-vars)
                    (apply where where-clauses))
         q (cond-> q-base
              (:limit opts) (limit (:limit opts))
              (:offset opts) (offset (:offset opts))
              (:order opts) (apply order-by (:order opts))
              (:distinct opts) (distinct))]
     (execute q db))))

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

(defn query-derivations
  "Query: Find all derivations from an entity
  SPARQL equivalent:
    SELECT ?derived
    WHERE {
      ?derived prov:wasDerivedFrom ?entity
    }"
  [db entity-id]
  (d/q '[:find [?derived]
         :in $ ?entity-id
         :where
         [?derived :prov/wasDerivedFrom ?entity-id]]
       db
       entity-id))

(defn query-activities
  "Query: Find all activities involving an entity
  SPARQL equivalent:
    SELECT ?activity
    WHERE {
      ?activity prov:used ?entity .
    }"
  [db entity-id]
  (d/q '[:find [?activity]
         :in $ ?entity-id
         :where
         [?activity :prov/used ?entity-id]]
       db
       entity-id))

(defn query-agent-activities
  "Query: Find all activities performed by an agent
  SPARQL equivalent:
    SELECT ?activity
    WHERE {
      ?activity prov:wasAssociatedWith ?agent .
    }"
  [db agent-id]
  (d/q '[:find [?activity ?time]
         :in $ ?agent-id
         :where
         [?activity :prov/wasAssociatedWith ?agent-id]
         [?activity :prov/startedAtTime ?time]]
       db
       agent-id))

(defn query-traceability-path
  "Query: Trace complete path from origin to destination
  Returns ordered list of entities and activities"
  [db start-entity-id end-entity-id]
  (d/q '[:find [?path-entity ?activity ?time]
         :in $ ?start ?end
         :where
         [?path-entity :prov/entity ?e]
         [?path-entity :prov/wasGeneratedBy ?activity]
         [?activity :prov/startedAtTime ?time]
         [(>= ?time #inst "2024-01-01T00:00:00Z")]
         [(<= ?time #inst "2025-12-31T23:59:59Z")]]
       db
       start-entity-id
       end-entity-id))

;; ============================================================================
;; Supply Chain Specific Queries
;; ============================================================================

(defn query-product-history
  "Query: Complete history of a product through supply chain
  Returns all events in chronological order"
  [db product-id]
  (d/q '[:find [?entity ?activity-type ?location ?time]
         :in $ ?product-id
         :where
         [?entity :prov/entity ?product-id]
         [?entity :prov/wasGeneratedBy ?activity]
         [?activity :prov/activity-type ?activity-type]
         [?activity :prov/startedAtTime ?time]
         [(get-else $ ?activity :traceability/location "unknown") ?location]]
       db
       product-id))

(defn query-batch-products
  "Query: Find all products in a batch"
  [db batch-number]
  (d/q '[:find [?product-id ?product-type]
         :in $ ?batch
         :where
         [?e :traceability/batch ?batch]
         [?e :prov/entity ?product-id]
         [?e :prov/entity-type ?product-type]]
       db
       batch-number))

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
;; Utility Functions
;; ============================================================================

(defn format-results
  "Format query results for display"
  [results]
  (mapv (fn [row]
          (into {} row))
        results))

(defn count-results
  "Count query results"
  [db query]
  (if (map? query)
    (count (d/q query db))
    (count query)))

(defn paginated-results
  "Get paginated results"
  [db query page-num page-size]
  (let [offset (* page-num page-size)]
    (d/q (assoc query :limit page-size :offset offset) db)))

;; ============================================================================
;; Development Helpers
;; ============================================================================

(comment
  ;; Development REPL usage
  (def db (dev/db))

  ;; Simple SPARQL-like query
  (execute-query (dev/conn)
                 [?e ?type]
                 [[?e :prov/entity-type ?type]]
                 {:limit 10})

  ;; Provenance query
  (query-provenance db some-entity-id)

  ;; Product history
  (query-product-history db some-product-id)

  ;; Supply chain path
  (query-supply-chain-path db product-id))
