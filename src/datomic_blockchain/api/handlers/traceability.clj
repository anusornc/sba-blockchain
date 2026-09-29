(ns datomic-blockchain.api.handlers.traceability
  "Traceability and provenance handlers"
  (:require [taoensso.timbre :as log]
            [datomic.api :as d]
            [datomic-blockchain.api.handlers.common :as common]
            [datomic-blockchain.query.provenance :as prov-query]
            [datomic-blockchain.query.graph :as graph]
            [datomic-blockchain.traceability.completeness :as completeness]
            [datomic-blockchain.traceability.journey :as journey]
            [datomic-blockchain.traceability.ui-contract :as ui-contract]))

(defn- request-param
  "Read a request param by keyword or string key."
  [params k]
  (or (get params k)
      (get params (name k))))

(defn- format-history-tuple
  [db [_entity-eid activity-eid activity-type agent-name time location]]
  (let [activity (d/entity db activity-eid)]
    {:activity-id (str (or (:prov/activity activity) activity-eid))
     :activity-type (str activity-type)
     :agent-name (str agent-name)
     :time (str time)
     :location (str location)}))

(defn- product-history
  [db product-uuid]
  (->> (or (prov-query/query-product-history db product-uuid) [])
       (mapv #(format-history-tuple db %))
       (sort-by :time)
       vec))

(defn- format-provenance-tuple
  [db [entity-eid activity-eid agent-eid time]]
  (let [entity (d/entity db entity-eid)
        activity (d/entity db activity-eid)
        agent (d/entity db agent-eid)]
    {:entity-id (str (:prov/entity entity))
     :entity-name (str (or (:traceability/product-name entity)
                           (:prov/entity-type entity)))
     :activity-type (str (:prov/activity-type activity))
     :agent-name (str (:prov/agent-name agent))
     :time (str time)}))

(defn- timeline-event
  [db eid]
  (let [e (d/entity db eid)
        start (or (:prov/startedAtTime e)
                  (when-let [act-id (:prov/wasGeneratedBy e)]
                    (:prov/startedAtTime (d/entity db [:prov/activity act-id]))))]
    {:id (str (or (:prov/entity e) (:prov/activity e) eid))
     :type (cond
             (:prov/entity e) :entity
             (:prov/activity e) :activity
             :else :unknown)
     :label (str (or (:traceability/product-name e)
                     (:prov/activity-type e)
                     (:prov/entity-type e)
                     eid))
     :timestamp (some-> start str)}))

(defn- unique-nodes
  [nodes]
  (vec (vals (reduce (fn [acc n]
                       (assoc acc (:id n) n))
                     {}
                     nodes))))

(defn- graph-for-product
  [db product-uuid history entity]
  (let [subgraph (graph/build-subgraph db product-uuid 3)
        product-node {:id (str product-uuid)
                      :label (str (or (:traceability/product-name entity)
                                      (:traceability/batch entity)
                                      product-uuid))
                      :type :entity}
        activity-nodes (mapv (fn [{:keys [activity-id activity-type]}]
                               {:id activity-id
                                :label activity-type
                                :type :activity})
                             history)
        history-edges (mapv (fn [{:keys [activity-id]}]
                              {:from activity-id
                               :to (str product-uuid)
                               :relation :prov/wasGeneratedBy})
                            history)]
    {:nodes (unique-nodes (concat (:nodes subgraph) [product-node] activity-nodes))
     :edges (vec (concat (:edges subgraph) history-edges))}))

(defn- completeness-fields
  "Completeness score and named missing evidence from the assessment
   interface, in the response-payload shape."
  [assessment]
  (select-keys assessment [:completeness-score :missing-evidence]))

;; ============================================================================
;; Traceability Handlers
;; ============================================================================

(defn handle-trace-product
  "Trace product through supply chain"
  [request connection]
  (common/with-error-handling "Trace product"
    (let [product-id (get-in request [:params :id])
          db (d/db connection)]
      (log/info "Trace product:" product-id)
      (if-let [product-uuid (prov-query/resolve-product-uuid db product-id)]
        (let [history (product-history db product-uuid)
              assessment (completeness/assess-completeness db product-uuid)]
          (common/success
           (merge {:product-id (str product-uuid)
                   :history history
                   :events (count history)}
                  (completeness-fields assessment))))
        (common/not-found "Product" product-id)))))

(defn handle-get-provenance
  "Get provenance information for entity"
  [request connection]
  (common/with-error-handling "Get provenance"
    (let [entity-id (get-in request [:params :id])
          db (d/db connection)]
      (log/info "Get provenance for:" entity-id)
      (let [entity-uuid (common/validate-uuid-param :id entity-id)
            provenance (->> (or (prov-query/query-provenance db entity-uuid) [])
                            (mapv #(format-provenance-tuple db %)))]
        (common/success
         {:entity-id entity-id
          :provenance provenance})))))

(defn handle-get-timeline
  "Get timeline visualization data"
  [request connection]
  (common/with-error-handling "Get timeline"
    (let [entity-id (get-in request [:params :id])
          db (d/db connection)]
      (log/info "Get timeline for:" entity-id)
      (let [entity-uuid (common/validate-uuid-param :id entity-id)
            events (->> (prov-query/timeline-eids db entity-uuid)
                        (map #(timeline-event db %))
                        (sort-by :timestamp)
                        vec)]
        (common/success
         {:entity-id entity-id
          :timeline events
          :event-count (count events)})))))

(defn handle-trace-by-qr
  "Trace product by QR code or batch ID from the Datomic ledger.
   Public endpoint (no authentication required)."
  [request connection]
  (common/with-error-handling "Trace by QR"
    (let [params (:params request)
          qr-code (request-param params :qr)
          batch-id (request-param params :batch)
          db (d/db connection)]
      (when (and (nil? qr-code) (nil? batch-id))
        (throw (ex-info "Missing query parameter: qr or batch"
                        {:error :missing-param
                         :status 400
                         :message "Missing query parameter: qr or batch"})))
      (if-let [entity-eid (prov-query/product-eid-by-qr-or-batch db qr-code batch-id)]
        (let [entity (d/entity db entity-eid)
              product-uuid (:prov/entity entity)
              history (product-history db product-uuid)
              stages (journey/stages-from-ledger db product-uuid)]
          (common/success
           (merge {:product {:batch (:traceability/batch entity)
                             :product (:traceability/product-name entity)
                             :qr-code (:traceability/qr-code entity)
                             :entity-id (str product-uuid)}
                   :journey {:stages stages}
                   :graph (graph-for-product db product-uuid history entity)}
                  (completeness-fields
                   (completeness/assess-completeness db product-uuid)))))
        (common/not-found "Product" (or qr-code batch-id))))))
(defn handle-ui-trace
  "Return the stable read-only frontend trace contract.
   Public endpoint for ontology traceability visualization."
  [request _connection]
  (common/with-error-handling "UI trace"
    (let [params (:params request)
          code (common/sanitize-string-param :code (common/get-val params :code) 128)
          kind (common/sanitize-string-param :kind (common/get-val params :kind) 32)]
      (when-not (seq code)
        (throw (ex-info "Missing trace code"
                        {:error :missing-code
                         :status 400
                         :message "Missing trace code"})))
      (common/success (ui-contract/build-trace-view code kind)))))

(defn handle-ui-ontology
  "Return the stable frontend ontology vocabulary used by the trace viewer."
  [_request _connection]
  (common/with-error-handling "UI ontology"
    (common/success (ui-contract/ontology-vocabulary))))
