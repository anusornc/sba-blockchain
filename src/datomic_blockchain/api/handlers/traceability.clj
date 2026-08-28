(ns datomic-blockchain.api.handlers.traceability
  "Traceability and provenance handlers"
  (:require [taoensso.timbre :as log]
            [datomic.api :as d]
            [datomic-blockchain.api.handlers.common :as common]
            [datomic-blockchain.query.sparql :as sparql]
            [datomic-blockchain.query.graph :as graph]
            [datomic-blockchain.traceability.confidence :as completeness])
  (:import [java.util UUID]))

(defn- request-param
  "Read a request param by keyword or string key."
  [params k]
  (or (get params k)
      (get params (name k))))

(defn- resolve-product-uuid
  "Resolve a product identifier (UUID string or batch id) to a PROV entity UUID."
  [db product-id]
  (or (common/parse-uuid-safe product-id)
      (when (uuid? product-id) product-id)
      (d/q '[:find ?pid .
             :in $ ?batch
             :where
             [?e :traceability/batch ?batch]
             [?e :prov/entity ?pid]]
           db
           product-id)))

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
  (->> (or (sparql/query-product-history db product-uuid) [])
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

(defn- find-product-eid
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

(defn- completeness-view
  "Public completeness score and named missing evidence for a provenance path."
  [db entity-id]
  (let [result (completeness/provenance-confidence db entity-id {})]
    (if (:error result)
      {:completeness-score 0.0
       :missing-evidence []}
      (let [best (get-in result [:confidence :best-path])
            score (double (or (:confidence best) 0.0))
            missing (->> (or (:hops best) [])
                         (mapcat :missing)
                         (map name)
                         distinct
                         vec)]
        {:completeness-score score
         :missing-evidence missing}))))

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
      (if-let [product-uuid (resolve-product-uuid db product-id)]
        (let [history (product-history db product-uuid)]
          (common/success
           (merge {:product-id (str product-uuid)
                   :history history
                   :events (count history)}
                  (completeness-view db product-uuid))))
        (common/success
         {:product-id product-id
          :history []
          :events 0
          :completeness-score 0.0
          :missing-evidence []})))))

(defn handle-get-provenance
  "Get provenance information for entity"
  [request connection]
  (common/with-error-handling "Get provenance"
    (let [entity-id (get-in request [:params :id])
          db (d/db connection)]
      (log/info "Get provenance for:" entity-id)
      (let [entity-uuid (UUID/fromString entity-id)
            provenance (->> (or (sparql/query-provenance db entity-uuid) [])
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
      (let [entity-uuid (UUID/fromString entity-id)
            self (d/q '[:find ?e .
                        :in $ ?id
                        :where [?e :prov/entity ?id]]
                      db entity-uuid)
            ancestors (or (graph/get-ancestors db entity-uuid) #{})
            descendants (or (graph/get-descendants db entity-uuid) #{})
            events (->> (cond-> (concat ancestors descendants)
                          self (conj self))
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
      (if-let [entity-eid (find-product-eid db qr-code batch-id)]
        (let [entity (d/entity db entity-eid)
              product-uuid (:prov/entity entity)
              history (product-history db product-uuid)
              stages (mapv (fn [{:keys [activity-type agent-name time location]}]
                             {:stage activity-type
                              :activity activity-type
                              :agent agent-name
                              :location location
                              :time time})
                           history)]
          (common/success
           (merge {:product {:batch (:traceability/batch entity)
                             :product (:traceability/product-name entity)
                             :qr-code (:traceability/qr-code entity)
                             :entity-id (str product-uuid)}
                   :journey {:stages stages}
                   :graph (graph-for-product db product-uuid history entity)}
                  (completeness-view db product-uuid))))
        (common/not-found "Product" (or qr-code batch-id))))))
