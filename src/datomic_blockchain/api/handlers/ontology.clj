(ns datomic-blockchain.api.handlers.ontology
  "Ontology management handlers"
  (:require [taoensso.timbre :as log]
            [datomic.api :as d]
            [datomic-blockchain.api.handlers.common :as common]
            [datomic-blockchain.ontology.loader :as loader]))

;; ============================================================================
;; Ontology Handlers
;; ============================================================================

(defn handle-list-ontologies
  "List loaded ontologies"
  [request connection]
  (common/with-error-handling "List ontologies"
    (log/info "List ontologies")
    (let [ontologies (vec (or (loader/list-ontologies connection) []))]
      (common/success
       {:ontologies ontologies
        :count (count ontologies)}))))

(defn handle-get-ontology
  "Get ontology structure from Datomic"
  [request connection]
  (common/with-error-handling "Get ontology"
    (let [ontology-id-param (get-in request [:params :id])
          ontology-id (common/validate-uuid-param :id ontology-id-param)]
      (log/info "Get ontology:" ontology-id)
      ;; Query actual ontology from Datomic
      (if-let [db (when connection (d/db connection))]
        ;; pull on an absent entity returns a truthy near-empty map,
        ;; so existence is checked through the entity itself
        (if-let [_entity (d/entity db ontology-id)]
          (let [ontology (d/pull db '[*] ontology-id)]
            (common/success
             {:ontology-id ontology-id
              :name (:ontology/name ontology)
              :classes (:ontology/classes ontology)
              :properties (:ontology/properties ontology)}))
          (common/not-found "Ontology" ontology-id))
        (common/error "Database not connected" 503)))))
