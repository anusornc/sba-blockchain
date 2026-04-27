(ns datomic-blockchain.api.handlers.traceability
  "Traceability and provenance handlers"
  (:require [clojure.string :as str]
            [taoensso.timbre :as log]
            [datomic.api :as d]
            [datomic-blockchain.api.handlers.common :as common]
            [datomic-blockchain.query.sparql :as sparql]
            [datomic-blockchain.query.graph :as graph]
            [datomic-blockchain.data.dataset-loader :as dataset])
  (:import [java.util UUID]))

(defn- batch-id->variant-key
  "Map a batch identifier to dataset variant key."
  [batch-id]
  (cond
    (str/starts-with? batch-id "UHT-CHOC") :chocolate
    (str/starts-with? batch-id "UHT-PLAIN") :plain
    (str/starts-with? batch-id "UHT-STRAW") :strawberry
    (or (str/starts-with? batch-id "MILK-THAI")
        (str/starts-with? batch-id "RAW-MILK")) :raw-milk
    :else nil))

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

      (let [history (sparql/query-product-history db product-id)]
        (common/success
         {:product-id product-id
          :history history
          :events (count history)})))))

(defn handle-get-provenance
  "Get provenance information for entity"
  [request connection]
  (common/with-error-handling "Get provenance"
    (let [entity-id (get-in request [:params :id])
          db (d/db connection)]

      (log/info "Get provenance for:" entity-id)

      (let [entity-uuid (UUID/fromString entity-id)
            provenance (sparql/query-provenance db entity-uuid)]

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
            ancestors (graph/get-ancestors db entity-uuid)
            descendants (graph/get-descendants db entity-uuid)
            all-events (concat ancestors descendants)]

        (common/success
         {:entity-id entity-id
          :timeline (sort-by :timestamp all-events)
          :event-count (count all-events)})))))

(defn handle-trace-by-qr
  "Trace product by QR code or batch ID
   Public endpoint (no authentication required)
   Query params: :qr - QR code, :batch - Batch ID
   Returns complete supply chain journey with graph data"
  [request connection]
  (common/with-error-handling "Trace by QR"
    (let [params (:params request)
          qr-code (:qr params)
          batch-id (:batch params)]

      (when (and (nil? qr-code) (nil? batch-id))
        (throw (ex-info "Missing query parameter: qr or batch"
                       {:error :missing-param
                        :status 400})))

      (let [ds (dataset/load-dataset)
            product (cond
                      qr-code (dataset/find-product-by-qr-code ds qr-code)
                      batch-id (dataset/find-product-by-batch-id ds batch-id)
                      :else nil)]

        (if (nil? product)
          (common/not-found "Product" (or qr-code batch-id))
          (let [actual-batch-id (:traceability/batch product)
                ;; Resolve supply chain journey from normalized batch prefix
                variant-key (batch-id->variant-key actual-batch-id)
                journey (when variant-key
                          (dataset/get-supply-chain-journey ds variant-key))]

            (if (nil? journey)
              (common/error "Journey not found for product" 404)
              ;; Build graph data from journey
              (let [graph-data (atom {:nodes [] :edges []})
                    graph-idx (atom 0)
                    node-ids (atom {})]

                ;; Process journey stages
                (doseq [stage journey]
                  (let [stage-name (name (:stage stage))
                        activity (:activity stage)
                        prod (:product stage)]
                    ;; Add activity node
                    (swap! node-ids assoc stage-name @graph-idx)
                    (swap! graph-data update :nodes conj
                           {:id stage-name
                            :label (:rdfs/label activity)
                            :type :activity})
                    (swap! graph-idx inc)
                    ;; Add product node
                    (when prod
                      (let [prod-name (:traceability/batch prod)]
                        (swap! node-ids assoc prod-name @graph-idx)
                        (swap! graph-data update :nodes conj
                               {:id prod-name
                                :label (or (:traceability/product prod)
                                           (:rdfs/label prod))
                                :type :entity})
                        (swap! graph-idx inc)
                        ;; Add edge
                        (swap! graph-data update :edges conj
                               {:from stage-name
                                :to prod-name
                                :relation :prov/wasGeneratedBy})))))

                ;; Build provenance and response
                (let [provenance {:stages (mapv (fn [stage]
                                                  {:stage (name (:stage stage))
                                                   :activity (:rdfs/label (:activity stage))
                                                   :location (get-in (:activity stage) [:uht/location])
                                                   :time (get-in (:activity stage) [:prov/startedAtTime])})
                                                journey)}]
                  (common/success
                   {:product {:batch (:traceability/batch product)
                              :product (or (:traceability/product product)
                                           (:rdfs/label product))
                              :variant-name (:uht/variant-name product)
                              :flavor (:uht/flavor product)
                              :qr-code (:uht/qr-code product)}
                    :journey provenance
                    :graph @graph-data}))))))))))
