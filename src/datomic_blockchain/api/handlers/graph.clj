(ns datomic-blockchain.api.handlers.graph
  "Graph visualization and traversal handlers"
  (:require [taoensso.timbre :as log]
            [datomic.api :as d]
            [datomic-blockchain.api.handlers.common :as common]
            [datomic-blockchain.query.graph :as graph]))

;; ============================================================================
;; Graph Handlers
;; ============================================================================

(defn handle-get-entity
  "Get entity with its relationships (with input validation)"
  [request connection]
  (common/with-error-handling "Get entity"
    (let [entity-id-param (get-in request [:params :id])
          entity-id (common/validate-uuid-param :id entity-id-param)
          depth (common/validate-positive-int :depth (get-in request [:params :depth]) 2)
          db (d/db connection)]

      (log/info "Get entity:" entity-id "depth:" depth)

      (let [entity (d/pull db '[*] entity-id)]
        (if entity
          (let [neighbors (graph/get-node-neighbors db entity-id depth)]
            (common/success
             {:entity entity
              :neighbors neighbors
              :depth depth}))
          (common/not-found "Entity" entity-id))))))

(defn handle-get-graph
  "Get graph data for visualization (with input validation)"
  [request connection]
  (common/with-error-handling "Get graph"
    (let [entity-id-param (get-in request [:params :id])
          entity-id (common/validate-uuid-param :id entity-id-param)
          depth (common/validate-positive-int :depth (get-in request [:params :depth]) 3)
          include-paths? (Boolean/parseBoolean (or (get-in request [:params :paths]) "false"))
          db (d/db connection)]

      (log/info "Get graph for entity:" entity-id "depth:" depth)

      (let [graph-data (graph/build-subgraph db entity-id depth)]
        (if include-paths?
          ;; Include paths from entity
          (let [paths (graph/find-all-paths db entity-id depth)]
            (common/success
             {:nodes (:nodes graph-data)
              :edges (:edges graph-data)
              :paths paths
              :depth depth}))
          ;; Just subgraph
          (common/success
           {:nodes (:nodes graph-data)
            :edges (:edges graph-data)
            :depth depth}))))))

(defn handle-find-path
  "Find path between two entities (with input validation)"
  [request connection]
  (common/with-error-handling "Find path"
    (let [start-id-param (get-in request [:params :from])
          end-id-param (get-in request [:params :to])
          _ (when-not (common/valid-uuid? start-id-param)
              (throw (ex-info "Invalid start entity ID format"
                             {:error :invalid-uuid
                              :param :from
                              :value start-id-param
                              :status 400})))
          _ (when-not (common/valid-uuid? end-id-param)
              (throw (ex-info "Invalid end entity ID format"
                             {:error :invalid-uuid
                              :param :to
                              :value end-id-param
                              :status 400})))
          start-id (java.util.UUID/fromString start-id-param)
          end-id (java.util.UUID/fromString end-id-param)
          db (d/db connection)]

      (log/info "Find path from" start-id "to" end-id)

      (let [path (graph/find-path db start-id end-id)]
        (if path
          (common/success
           {:path path
            :length (count path)
            :from start-id
            :to end-id})
          (common/error "No path found" 404))))))

;; ============================================================================
;; Stats Handler
;; ============================================================================

(defn handle-get-stats
  "Get system statistics"
  [request connection]
  (log/info "Get statistics")
  (let [db (d/db connection)]
    (common/success
     {:knowledge-base {:total-entities (first (d/q '[:find (count ?e)
                                                     :where [?e :prov/entity]] db))
                       :total-activities (first (d/q '[:find (count ?a)
                                                       :where [?a :prov/activity]] db))}
      :timestamp (java.util.Date.)})))
