(ns datomic-blockchain.usecases.supply-chain
  "Supply chain traceability use case implementation"
  (:require [datomic.api :as d]
            [datomic-blockchain.data.dataset-loader :as dataset]
            [datomic-blockchain.query.graph :as graph]
            [taoensso.timbre :as log]
            [clojure.data.json :as json]
            [clojure.string :as str])
  (:import [java.util UUID Date]))

;; ============================================================================
;; Demo Functions
;; ============================================================================

(defn run-demo
  "Run a complete supply chain demo using UHT dataset
   Returns map with demo results"
  [conn]
  (log/info "Running supply chain demo")
  (let [ds (dataset/load-dataset)
        product (dataset/get-product ds :chocolate)
        batch-id (:traceability/batch product)
        product-name (:uht/variant-name product)
        
        ;; Get the supply chain journey
        journey (dataset/get-supply-chain-journey ds :chocolate)
        
        ;; Create demo entities in the database
        farmer-agent (:farmer (:agents ds))
        manufacturer-agent (:manufacturer (:agents ds))
        
        ;; Generate product UUID
        product-uuid (UUID/randomUUID)
        
        ;; Transaction 1: Create product at farm
        _ @(d/transact conn 
                       [{:db/id "temp-product"
                         :prov/entity product-uuid
                         :prov/entity-type :product/chocolate-uht
                         :traceability/batch batch-id
                         :traceability/product-name product-name}
                        {:db/id "temp-farmer"
                         :prov/agent (UUID/randomUUID)
                         :prov/agent-name (:uht/agent-name farmer-agent)
                         :prov/agent-type :organization/farmer}])]
  
  ;; Transaction 2: Processing activity
  @(d/transact conn
                 [{:db/id "temp-processing"
                   :prov/activity (UUID/randomUUID)
                   :prov/activity-type :supply-chain/uht-processing
                   :prov/startedAtTime (Date.)
                   :prov/used product-uuid}
                  {:db/id "temp-manufacturer"
                   :prov/agent (UUID/randomUUID)
                   :prov/agent-name (:uht/agent-name manufacturer-agent)
                   :prov/agent-type :organization/manufacturer}])
    
    ;; Transaction 3: Transport
    @(d/transact conn
                 [{:db/id "temp-transport"
                   :prov/activity (UUID/randomUUID)
                   :prov/activity-type :supply-chain/transport
                   :prov/startedAtTime (Date.)
                   :prov/used product-uuid}])
    
    (log/info "Supply chain demo completed for batch:" batch-id)
    {:product-name product-name
     :product-id batch-id
     :events (count journey)
     :duration-days 5
     :journey journey}))

(defn generate-supply-chain-report
  "Generate a markdown report for a supply chain"
  [conn product-id]
  (let [db (d/db conn)
        ;; Find the product
        product-eid (ffirst (d/q '[:find ?e 
                                   :in $ ?batch
                                   :where [?e :traceability/batch ?batch]]
                                 db product-id))
        
        product-data (when product-eid
                       (d/entity db product-eid))
        
        ;; Get provenance
        activities (when product-eid
                     (d/q '[:find ?activity ?type ?time
                            :in $ ?product
                            :where
                            [?activity :prov/used ?product]
                            [?activity :prov/activity-type ?type]
                            [?activity :prov/startedAtTime ?time]]
                          db product-eid))]
    
    (str "# Supply Chain Report\n\n"
         "**Product ID:** " product-id "\n\n"
         "**Generated:** " (Date.) "\n\n"
         "---\n\n"
         "## Product Information\n\n"
         "- **Name:** " (get product-data :traceability/product "Unknown") "\n"
         "- **Batch:** " (get product-data :traceability/batch "N/A") "\n"
         "- **Status:** " (get product-data :traceability/status "Unknown") "\n\n"
         "---\n\n"
         "## Provenance Chain\n\n"
         (if (seq activities)
           (str "| Stage | Activity | Timestamp |\n"
                "|-------|----------|-----------|\n"
                (str/join "\n"
                          (map (fn [[_ type time]]
                                 (format "| %s | %s | %s |"
                                         (name (get type :stage "unknown"))
                                         (name type)
                                         time))
                               (sort-by #(nth % 2) activities))))
           "No activities recorded.\n")
         "\n\n---\n\n"
         "## Agents\n\n"
         "Supply chain participants tracked in provenance.\n\n"
         "---\n\n"
         "*Report generated by Datomic Blockchain Supply Chain System*")))

(defn visualize-supply-chain
  "Generate graph data for supply chain visualization
   Returns map with nodes and edges for cytoscape.js"
  [conn product-id]
  (let [db (d/db conn)
        product-eid (ffirst (d/q '[:find ?e 
                                   :in $ ?batch
                                   :where [?e :traceability/batch ?batch]]
                                 db product-id))
        
        ;; Get all related entities
        related (when product-eid
                  (d/q '[:find ?e ?attr ?val
                         :in $ ?product
                         :where
                         [?e ?attr ?val]
                         (or [?e :prov/used ?product]
                             [?product :prov/wasGeneratedBy ?e]
                             [?product :prov/wasAssociatedWith ?e])]
                       db product-eid))
        
        ;; Build nodes
        nodes (into [{:data {:id (str product-eid)
                            :label product-id
                            :type :product}}]
                    (map (fn [[eid attr _]]
                           {:data {:id (str eid)
                                   :label (name attr)
                                   :type :activity}})
                         related))
        
        ;; Build edges
        edges (map (fn [[eid attr _]]
                     {:data {:id (str eid "-" product-eid)
                             :source (str eid)
                             :target (str product-eid)
                             :label (name attr)}})
                   related)]
    
    {:nodes nodes
     :edges edges}))

(defn validate-supply-chain
  "Validate a supply chain for completeness
   Returns map with validation results"
  [conn product-id]
  (let [db (d/db conn)
        product-eid (ffirst (d/q '[:find ?e 
                                   :in $ ?batch
                                   :where [?e :traceability/batch ?batch]]
                                 db product-id))
        
        ;; Check if product exists
        exists? (some? product-eid)
        
        ;; Get activities
        activities (when product-eid
                     (d/q '[:find ?activity ?type
                            :in $ ?product
                            :where
                            [?activity :prov/used ?product]
                            [?activity :prov/activity-type ?type]]
                          db product-eid))
        
        ;; Check for gaps (simplified - should have at least 3 stages)
        has-enough-stages? (>= (count activities) 3)]
    
    {:product-exists exists?
     :complete-provenance has-enough-stages?
     :no-gaps has-enough-stages?
     :activity-count (count activities)}))

;; ============================================================================
;; UHT-Specific Functions
;; ============================================================================

(defn load-full-uht-supply-chain!
  "Load the complete UHT milk supply chain from dataset into database"
  [conn]
  (log/info "Loading full UHT supply chain into database")
  (let [ds (dataset/load-dataset)
        
        ;; Step 1: Load all agents
        agent-tx (mapv (fn [[key data]]
                        {:db/id (str "agent-" (name key))
                         :prov/agent (UUID/randomUUID)
                         :prov/agent-name (or (:uht/agent-name data) "Unknown Agent")
                         :prov/agent-type (case key
                                           :farmer :organization/farmer
                                           :manufacturer :organization/manufacturer
                                           :logistics :organization/logistics
                                           :retailer :organization/retailer
                                           :consumer :organization/consumer
                                           :organization/other)
                         :traceability/location (or (:uht/location data) "Unknown Location")
                         :traceability/certifications (vec (or (:uht/certifications data) []))})
                      (:agents ds))
        
        _ @(d/transact conn agent-tx)
        
        ;; Step 2: Load all products
        product-tx (mapv (fn [[key data]]
                          {:db/id (str "product-" (name key))
                           :prov/entity (UUID/randomUUID)
                           :prov/entity-type (case key
                                               :raw-milk :product/raw-milk
                                               :chocolate :product/chocolate-uht
                                               :plain :product/plain-uht
                                               :strawberry :product/strawberry-uht)
                           :traceability/batch (or (:traceability/batch data) "UNKNOWN")
                           :traceability/product (UUID/randomUUID)
                           :traceability/product-name (or (:traceability/product data)
                                                         (:uht/variant-name data)
                                                         "Unknown Product")})
                        (:products ds))
        
        _ @(d/transact conn product-tx)
        
        ;; Step 3: Load activities
        activity-tx (mapv (fn [[key data]]
                           {:db/id (str "activity-" (name key))
                            :prov/activity (UUID/randomUUID)
                            :prov/activity-type (case key
                                                 :milking :activity/milking
                                                 :uht-processing :activity/uht-processing
                                                 :chocolate-processing :activity/chocolate-processing
                                                 :plain-processing :activity/plain-processing
                                                 :strawberry-processing :activity/strawberry-processing
                                                 :transport :activity/transport
                                                 :retail-sale :activity/retail-sale)
                            :prov/startedAtTime (Date.)})
                         (:activities ds))]
    
    @(d/transact conn activity-tx)
    
    (log/info "UHT supply chain loaded:"
              (count agent-tx) "agents,"
              (count product-tx) "products,"
              (count activity-tx) "activities")
    
    {:agents (count agent-tx)
     :products (count product-tx)
     :activities (count activity-tx)}))

(comment
  ;; Run the complete demo
  (def conn (datomic-blockchain.datomic.connection/connect
              (datomic-blockchain.config/load-config)))
  
  ;; Load full UHT supply chain
  (load-full-uht-supply-chain! conn)
  
  ;; Run demo
  (run-demo conn)
  
  ;; Generate report
  (generate-supply-chain-report conn "UHT-CHOC-2024-001")
  )
