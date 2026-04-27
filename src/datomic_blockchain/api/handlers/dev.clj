(ns datomic-blockchain.api.handlers.dev
  "Development and sample data handlers"
  (:require [clojure.instant :as instant]
            [clojure.string :as str]
            [datomic.api :as d]
            [datomic-blockchain.api.handlers.common :as common]
            [datomic-blockchain.data.dataset-loader :as dataset]
            [taoensso.timbre :as log])
  (:import [java.util Date UUID]))

;; ============================================================================
;; Dev Sample Data Handler
;; ============================================================================

(def ^:private dataset-path "resources/datasets/uht-supply-chain/data.edn")

(defn- stable-uuid
  "Generate deterministic UUIDs for benchmark seed entities."
  [id-str]
  (UUID/nameUUIDFromBytes (.getBytes (str "uht-seed/" id-str) "UTF-8")))

(defn- as-date
  "Parse ISO-8601 string into java.util.Date."
  [v]
  (cond
    (instance? Date v) v
    (string? v) (instant/read-instant-date v)
    :else nil))

(defn- normalize-location
  "Convert dataset location map into a compact string for schema compatibility."
  [location]
  (cond
    (string? location) location
    (map? location)
    (let [address (:address location)
          district (:district location)
          province (:province location)]
      (or address
          (some->> [district province]
                   (remove nil?)
                   seq
                   (str/join ", "))
          "Unknown Location"))
    :else "Unknown Location"))

(defn- agent-type-for-key
  [k]
  (case k
    :farmer :organization/farmer
    :manufacturer :organization/manufacturer
    :logistics :organization/logistics
    :retailer :organization/retailer
    :consumer :organization/consumer
    :organization/other))

(defn- entity-type-for-key
  [k]
  (case k
    :raw-milk :product/raw-milk
    :chocolate :product/chocolate-uht
    :plain :product/plain-uht
    :strawberry :product/strawberry-uht
    :product/unknown))

(defn- activity-type-for-key
  [k]
  (case k
    :milking :activity/milking
    :uht-processing :activity/uht-processing
    :uht-chocolate-processing :activity/chocolate-processing
    :uht-plain-processing :activity/plain-processing
    :uht-strawberry-processing :activity/strawberry-processing
    :transport :activity/transport
    :retail-sale :activity/retail-sale
    :activity/unknown))

(defn- related-activity-for-variant
  [variant]
  (case variant
    :chocolate :uht-chocolate-processing
    :plain :uht-plain-processing
    :strawberry :uht-strawberry-processing
    nil))

(defn- mk-relationship-tx
  [agent-ids product-ids activity-ids]
  (let [raw-id (get product-ids :raw-milk)
        transport-id (get activity-ids :transport)
        retail-id (get activity-ids :retail-sale)
        variant-keys [:chocolate :plain :strawberry]
        common-links
        [[:db/add [:prov/activity (get activity-ids :milking)]
          :prov/wasAssociatedWith
          (get agent-ids :farmer)]
         [:db/add [:prov/entity raw-id]
          :prov/wasGeneratedBy
          (get activity-ids :milking)]]
        variant-links
        (mapcat
         (fn [variant]
           (let [product-id (get product-ids variant)
                 variant-activity-key (related-activity-for-variant variant)
                 variant-activity-id (get activity-ids variant-activity-key)]
             [[:db/add [:prov/activity variant-activity-id]
               :prov/used
               raw-id]
              [:db/add [:prov/activity variant-activity-id]
               :prov/wasAssociatedWith
               (get agent-ids :manufacturer)]
              [:db/add [:prov/entity product-id]
               :prov/wasGeneratedBy
               variant-activity-id]
              [:db/add [:prov/entity product-id]
               :prov/wasDerivedFrom
               raw-id]
              [:db/add [:prov/activity transport-id]
               :prov/used
               product-id]
              [:db/add [:prov/activity retail-id]
               :prov/used
               product-id]]))
         variant-keys)
        logistics-links
        [[:db/add [:prov/activity transport-id]
          :prov/wasAssociatedWith
          (get agent-ids :logistics)]
         [:db/add [:prov/activity retail-id]
          :prov/wasAssociatedWith
          (get agent-ids :retailer)]]]
    (vec (concat common-links variant-links logistics-links))))

(defn handle-load-sample-data
  "Load deterministic sample supply chain data from the canonical UHT dataset."
  [_request connection]
  (common/with-error-handling "Load sample data"
    (let [conn connection
          ds (dataset/load-dataset dataset-path)
          agent-ids (into {} (map (fn [k] [k (stable-uuid (str "agent/" (name k)))]) (keys (:agents ds))))
          product-ids (into {} (map (fn [k] [k (stable-uuid (str "entity/" (name k)))]) (keys (:products ds))))
          activity-ids (into {} (map (fn [k] [k (stable-uuid (str "activity/" (name k)))]) (keys (:activities ds))))
          agent-tx
          (mapv (fn [[k data]]
                  (let [agent-id (get agent-ids k)
                        certs (or (:uht/certifications data) #{})
                        cert-strings (mapv (fn [c] (if (keyword? c) (name c) (str c))) certs)]
                    {:db/id (str "agent-" (name k))
                     :prov/agent agent-id
                     :prov/agent-name (or (:uht/agent-name data) "Unknown Agent")
                     :prov/agent-type (or (:uht/agent-type data) (agent-type-for-key k))
                     :traceability/location (normalize-location (:uht/location data))
                     :traceability/certifications cert-strings}))
                (:agents ds))
          product-tx
          (mapv (fn [[k data]]
                  (let [entity-id (get product-ids k)
                        product-id (stable-uuid (str "product/" (name k)))]
                    {:db/id (str "product-" (name k))
                     :prov/entity entity-id
                     :prov/entity-type (entity-type-for-key k)
                     :traceability/batch (or (:traceability/batch data) "UNKNOWN")
                     :traceability/product product-id
                     :traceability/product-name (or (:traceability/product data)
                                                   (:uht/variant-name data)
                                                   "Unknown Product")}))
                (:products ds))
          activity-tx
          (mapv (fn [[k data]]
                  (let [activity-id (get activity-ids k)
                        started-at (or (as-date (:prov/startedAtTime data)) (Date.))
                        ended-at (as-date (:prov/endedAtTime data))]
                    (cond-> {:db/id (str "activity-" (name k))
                             :prov/activity activity-id
                             :prov/activity-type (activity-type-for-key k)
                             :prov/startedAtTime started-at}
                      ended-at (assoc :prov/endedAtTime ended-at))))
                (:activities ds))
          relationship-tx (mk-relationship-tx agent-ids product-ids activity-ids)
          chocolate-data (get-in ds [:products :chocolate])
          benchmark-batch (or (:traceability/batch chocolate-data) "UHT-CHOC-CM-2024-001")
          benchmark-qr (or (:uht/qr-code chocolate-data) "UHT-CHOC-2024-001-QR")
          benchmark-entity-id (str (get product-ids :chocolate))
          benchmark-activity-id (str (get activity-ids :uht-chocolate-processing))]
      @(d/transact conn agent-tx)
      @(d/transact conn product-tx)
      @(d/transact conn activity-tx)
      @(d/transact conn relationship-tx)

      (log/info "Loaded deterministic UHT sample data"
                {:agents (count agent-tx)
                 :products (count product-tx)
                 :activities (count activity-tx)
                 :relationships (count relationship-tx)})

      (common/success
       {:message "Sample data loaded successfully"
        :dataset-source dataset-path
        :dataset-meta (:meta ds)
        :counts {:agents (count agent-tx)
                 :products (count product-tx)
                 :activities (count activity-tx)
                 :relationships (count relationship-tx)}
        :benchmark-anchors {:qr-code benchmark-qr
                            :batch-id benchmark-batch
                            :entity-id benchmark-entity-id
                            :activity-id benchmark-activity-id}
        :products [{:batch-id benchmark-batch
                    :name (:traceability/product chocolate-data)
                    :stages ["Milking"
                             "Chocolate Processing"
                             "Cold Chain Transport"
                             "Retail Sale"]}]}))))

;; ============================================================================
;; Dev Test Blockchain Handler
;; ============================================================================

(defn handle-create-test-blocks
  "Create test blockchain transactions for Block Explorer testing"
  [request connection]
  (common/with-error-handling "Create test blocks"
    (let [conn connection
          count (or (some-> (:params request) (get "count") parse-long) 3)
          now (java.util.Date.)
          results (atom [])]
      ;; Create simple blockchain transactions for testing
      (doseq [i (range count)]
        (let [tx-id (UUID/randomUUID)
              prev-hash (if (zero? i)
                          "00000000-0000-0000-0000-000000000000"
                          (:hash (last @results)))
              timestamp (java.util.Date. (- (.getTime now) (* (- count i 1) 60000)))
              nonce (rand-int 1000000)
              hash (str (UUID/randomUUID))]
          ;; Transact the blockchain transaction
          @(d/transact conn
                       [{:db/id "temp-tx"
                         :blockchain/transaction tx-id
                         :blockchain/timestamp timestamp
                         :blockchain/hash hash
                         :blockchain/previous-hash prev-hash
                         :blockchain/nonce nonce
                         :blockchain/creator (UUID/randomUUID)}])
          (swap! results conj {:tx-id tx-id
                               :hash hash
                               :nonce nonce
                               :timestamp (.toString timestamp)})))
      (common/success
       {:message (str "Created " count " test blockchain transactions")
        :blocks-created count
        :blocks @results}))))
