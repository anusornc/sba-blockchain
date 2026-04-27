(ns datomic-blockchain.api.handlers.blocks
  "Block explorer handlers for blockchain visualization"
  (:require [taoensso.timbre :as log]
            [datomic.api :as d]
            [datomic-blockchain.api.handlers.common :as common]))

;; ============================================================================
;; Block Explorer Handlers
;; ============================================================================

(defn handle-list-blocks
  "List all blockchain transactions (blocks) with pagination

   Query params:
   - offset: Starting position (default 0)
   - limit: Items per page (default 10, max 100)

   Returns paginated list of transactions with block info"
  [request connection]
  (common/with-error-handling "List blocks"
    (log/info "List blocks")
    (let [{:keys [offset limit]} (common/parse-pagination request)
          db (d/db connection)]

      ;; Query all blockchain transactions with pagination
      (let [all-txs (or (d/q '[:find [(pull ?tx [:blockchain/transaction
                                                  :blockchain/timestamp
                                                  :blockchain/hash
                                                  :blockchain/previous-hash
                                                  :blockchain/creator
                                                  :blockchain/nonce])]
                              :where [?tx :blockchain/transaction]]
                            db)
                       [])
            total-count (count all-txs)
            txs (if (pos? total-count)
                  (subvec all-txs offset (min (+ offset limit) total-count))
                  [])
            blocks (mapv (fn [tx-data]
                           (let [tx-id (:blockchain/transaction tx-data)
                                 timestamp (:blockchain/timestamp tx-data)
                                 hash (:blockchain/hash tx-data)
                                 prev-hash (:blockchain/previous-hash tx-data)
                                 creator (:blockchain/creator tx-data)
                                 nonce (:blockchain/nonce tx-data)]
                             {:block/number (str tx-id)
                              :block/hash (or hash "N/A")
                              :block/previous-hash (or prev-hash "N/A")
                              :block/timestamp (str timestamp)
                              :block/miner (str creator)
                              :block/nonce nonce
                              :block/transaction-count 1
                              :block/size (count (pr-str tx-data))}))
                         txs)]
        (common/success
         {:blocks blocks
          :pagination {:offset offset
                       :limit limit
                       :total total-count
                       :count (count blocks)}})))))

(defn handle-get-block
  "Get detailed information about a specific block (transaction)

   Path params:
   - id: Transaction UUID

   Returns full block details with all transaction data"
  [request connection]
  (common/with-error-handling "Get block"
    (log/info "Get block details")
    (let [params (:params request)
          block-id (get params :id)
          tx-id (common/parse-uuid-safe block-id)]

      (if (nil? tx-id)
        (common/validation-error "Invalid block ID format" {:format "UUID"})

        (let [db (d/db connection)
              entity (d/pull db '[*] tx-id)]

          (if (nil? entity)
            (common/not-found "Block" block-id)

            (let [tx-data (:blockchain/transaction entity)
                  timestamp (:blockchain/timestamp entity)
                  hash (:blockchain/hash entity)
                  prev-hash (:blockchain/previous-hash entity)
                  creator (:blockchain/creator entity)
                  signature (:blockchain/signature entity)
                  nonce (:blockchain/nonce entity)
                  data (:blockchain/data entity)]

              (common/success
               {:block/number (str tx-id)
                :block/hash (or hash "N/A")
                :block/previous-hash (or prev-hash "N/A")
                :block/timestamp (str timestamp)
                :block/miner (str creator)
                :block/nonce nonce
                :block/signature (or signature "N/A")
                :block/size (when data (count data))
                :block/transactions [{:tx/id (str tx-id)
                                      :tx/hash hash
                                      :tx/creator (str creator)
                                      :tx/timestamp (str timestamp)}]}))))))))
