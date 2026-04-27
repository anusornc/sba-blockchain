(ns datomic-blockchain.performance.test-data
  "Test data generation for benchmarks and demos"
  (:require [datomic.api :as d]
            [datomic-blockchain.data.dataset-loader :as dataset]
            [taoensso.timbre :as log]
            [clojure.string :as str])
  (:import [java.util UUID Date]))

;; ============================================================================
;; Supply Chain Test Data
;; ============================================================================

(defn create-agent
  "Create a PROV-O agent entity"
  [agent-key agent-data]
  (let [agent-id (UUID/randomUUID)]
    {:db/id (str "agent-" (name agent-key))
     :prov/agent agent-id
     :prov/agent-name (:uht/agent-name agent-data)
     :prov/agent-type (keyword "organization" (name agent-key))
     :prov/location (:uht/location agent-data)
     :blockchain/public-key (or (:blockchain/public-key agent-data)
                               (str "0x" (str/replace (str agent-id) "-" "")))}))

(defn create-product-entity
  "Create a PROV-O entity for a product"
  [product-key product-data agent-id]
  (let [entity-id (UUID/randomUUID)
        batch-id (:traceability/batch product-data)]
    {:db/id (str "product-" (name product-key))
     :prov/entity entity-id
     :prov/entity-type (keyword "product" (name product-key))
     :traceability/batch batch-id
     :traceability/product (:traceability/product product-data)
     :traceability/qr-code (:uht/qr-code product-data)
     :traceability/origin (:traceability/origin product-data)
     :traceability/quality-grade (:traceability/quality-grade product-data)}))

(defn create-activity
  "Create a PROV-O activity"
  [activity-key activity-data used-entity-id]
  (let [activity-id (UUID/randomUUID)]
    {:db/id (str "activity-" (name activity-key))
     :prov/activity activity-id
     :prov/activity-type (keyword "supply-chain" (name activity-key))
     :prov/startedAtTime (Date.)
     :prov/used used-entity-id
     :activity/location (:uht/location activity-data)}))

(defn load-supply-chain-data!
  "Load supply chain test data into database
   Returns map with :loaded count"
  [conn count]
  (log/info "Loading supply chain test data:" count "items")
  (let [ds (dataset/load-dataset)
        agents (take count (vals (:agents ds)))
        products (take count (vals (:products ds)))
        
        ;; Create agent transactions
        agent-tx (map (fn [[k v]] (create-agent k v)) (take count (:agents ds)))
        
        ;; Transact agents first
        _ (when (seq agent-tx)
            @(d/transact conn agent-tx))
        
        ;; Create product entities
        product-tx (map (fn [[k v]] 
                         (create-product-entity k v nil))
                       (take count (:products ds)))]
    
    ;; Transact products
    (when (seq product-tx)
      @(d/transact conn product-tx))
    
    (log/info "Loaded" (min count (+ (count agent-tx) (count product-tx))) "entities")
    {:loaded (min count (+ (count agent-tx) (count product-tx)))
     :agents (count agent-tx)
     :products (count product-tx)}))

;; ============================================================================
;; Synthetic Data Generation
;; ============================================================================

(defn generate-supply-chain-batch
  "Generate a single supply chain batch with full provenance"
  [batch-num]
  (let [batch-id (format "BATCH-%06d" batch-num)
        farmer-id (UUID/randomUUID)
        processor-id (UUID/randomUUID)
        logistics-id (UUID/randomUUID)
        retailer-id (UUID/randomUUID)
        product-id (UUID/randomUUID)]
    
    {:batch-id batch-id
     :agents {:farmer {:prov/agent farmer-id
                       :prov/agent-name (format "Farmer-%03d" batch-num)
                       :prov/agent-type :organization/farmer}
              :processor {:prov/agent processor-id
                          :prov/agent-name (format "Processor-%03d" batch-num)
                          :prov/agent-type :organization/processor}
              :logistics {:prov/agent logistics-id
                          :prov/agent-name (format "Logistics-%03d" batch-num)
                          :prov/agent-type :organization/logistics}
              :retailer {:prov/agent retailer-id
                         :prov/agent-name (format "Retailer-%03d" batch-num)
                         :prov/agent-type :organization/retailer}}
     :product {:prov/entity product-id
               :prov/entity-type :product/batch
               :traceability/batch batch-id
               :traceability/product "Generic Product"
               :traceability/origin (rand-nth ["Thailand" "Vietnam" "Indonesia" "Malaysia"])
               :traceability/quality-grade (rand-nth ["A" "B" "C" "Premium"])}
     :activities [{:prov/activity (UUID/randomUUID)
                   :prov/activity-type :supply-chain/production
                   :prov/startedAtTime (Date. (- (System/currentTimeMillis) 86400000))
                   :stage :farm}
                  {:prov/activity (UUID/randomUUID)
                   :prov/activity-type :supply-chain/processing
                   :prov/startedAtTime (Date. (- (System/currentTimeMillis) 43200000))
                   :stage :processing}
                  {:prov/activity (UUID/randomUUID)
                   :prov/activity-type :supply-chain/transport
                   :prov/startedAtTime (Date. (- (System/currentTimeMillis) 21600000))
                   :stage :logistics}
                  {:prov/activity (UUID/randomUUID)
                   :prov/activity-type :supply-chain/retail
                   :prov/startedAtTime (Date.)
                   :stage :retail}]}))

(defn load-synthetic-supply-chains!
  "Generate and load synthetic supply chain data"
  [conn n]
  (log/info "Generating" n "synthetic supply chains")
  (let [results (atom {:loaded 0 :errors 0})]
    (doseq [i (range n)]
      (try
        (let [batch (generate-supply-chain-batch i)
              ;; Transact agents
              agent-tx (mapv (fn [[_ agent-data]] 
                              (assoc agent-data :db/id (str "temp-agent-" i)))
                            (:agents batch))
              _ @(d/transact conn agent-tx)
              
              ;; Transact product
              product-tx [(assoc (:product batch) :db/id (str "temp-product-" i))]
              _ @(d/transact conn product-tx)]
          (swap! results update :loaded + 1))
        (catch Exception e
          (log/warn "Error loading batch" i ":" (.getMessage e))
          (swap! results update :errors inc))))
    @results))

(comment
  ;; Usage examples
  (def conn (datomic-blockchain.datomic.connection/connect 
              (datomic-blockchain.config/load-config)))
  
  ;; Load from UHT dataset
  (load-supply-chain-data! conn 10)
  
  ;; Generate synthetic data
  (load-synthetic-supply-chains! conn 50)
  )
