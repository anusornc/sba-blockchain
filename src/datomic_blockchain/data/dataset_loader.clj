(ns datomic-blockchain.data.dataset-loader
  "EDN Dataset Loader for UHT Milk Supply Chain

   Loads structured supply chain data from EDN files.
   EDN (Extensible Data Notation) is native to Clojure and human-readable."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [taoensso.timbre :as log]))

;; ============================================================================
;; Dataset Loading
;; ============================================================================

(defn load-dataset
  "Load the UHT milk supply chain dataset from EDN file

   Returns:
   {:agents {key => agent-data}
    :products {key => product-data}
    :activities {key => activity-data}}"
  ([]
   (load-dataset "resources/datasets/uht-supply-chain/data.edn"))
  ([file-path]
   (log/info "Loading dataset from:" file-path)
   (let [data (edn/read-string (slurp file-path))]
     (log/info "Dataset loaded:"
               (count (:agents data)) "agents,"
               (count (:products data)) "products,"
               (count (:activities data)) "activities")
     data)))

;; ============================================================================
;; Data Access Helpers
;; ============================================================================

(defn get-all-agents
  "Get all agents from dataset"
  [dataset]
  (:agents dataset))

(defn get-all-products
  "Get all products from dataset"
  [dataset]
  (:products dataset))

(defn get-all-activities
  "Get all activities from dataset"
  [dataset]
  (:activities dataset))

(defn get-agent
  "Get agent by key (e.g., :farmer, :manufacturer)"
  [dataset agent-key]
  (get-in dataset [:agents agent-key]))

(defn get-product
  "Get product by key (e.g., :raw-milk, :chocolate)"
  [dataset product-key]
  (get-in dataset [:products product-key]))

(defn get-activity
  "Get activity by key (e.g., :milking, :transport)"
  [dataset activity-key]
  (get-in dataset [:activities activity-key]))

(defn find-product-by-batch-id
  "Find a product by its batch ID"
  [dataset batch-id]
  (first (filter #(= (get % :traceability/batch) batch-id)
                 (vals (:products dataset)))))

(defn find-product-by-qr-code
  "Find a product by its QR code"
  [dataset qr-code]
  (first (filter #(= (get % :uht/qr-code) qr-code)
                 (vals (:products dataset)))))

(defn get-product-variants
  "Get all UHT product variants (chocolate, plain, strawberry)"
  [dataset]
  (filter #(contains? #{:chocolate :plain :strawberry}
                   (keyword (last (str/split (name (key %)) #":" 2))))
          (:products dataset)))

(defn get-supply-chain-journey
  "Get the complete supply chain journey for a product variant

   Returns ordered activities: milking → processing → transport → retail"
  [dataset variant-key]
  (let [product (get-product dataset variant-key)]
    (when product
      [{:stage :farm
        :activity (get-activity dataset :milking)
        :product (get-product dataset :raw-milk)}
       {:stage :manufacturing
        :activity (get-activity dataset :uht-processing)
        :product product}
       {:stage :logistics
        :activity (get-activity dataset :transport)
        :product product}
       {:stage :retail
        :activity (get-activity dataset :retail-sale)
        :product product}])))

(comment
  ;; Usage examples
  (def dataset (load-dataset))

  ;; Get all agents
  (get-all-agents dataset)

  ;; Get farmer data
  (get-agent dataset :farmer)

  ;; Get chocolate product
  (get-product dataset :chocolate)

  ;; Find by batch ID
  (find-product-by-batch-id dataset "MILK-THAI-2024-001")

  ;; Find by QR code
  (find-product-by-qr-code dataset "UHT-CHOC-2024-001-QR")

  ;; Get full journey
  (get-supply-chain-journey dataset :chocolate)
  )
