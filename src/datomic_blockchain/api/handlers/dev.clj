(ns datomic-blockchain.api.handlers.dev
  "Development and sample data handlers"
  (:require [datomic.api :as d]
            [datomic-blockchain.api.handlers.common :as common]
            [datomic-blockchain.traceability.sample-data :as sample]
            [taoensso.timbre :as log])
  (:import [java.util UUID]))

;; ============================================================================
;; Dev Sample Data Handler
;; =============================================================================

(defn handle-load-sample-data
  "Load deterministic sample supply chain data from the canonical UHT dataset."
  [_request connection]
  (common/with-error-handling "Load sample data"
    (common/success (assoc (sample/seed-uht-sample! connection)
                           :message "Sample data loaded successfully"))))

;; ============================================================================
;; Dev Test Blockchain Handler
;; =============================================================================

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
