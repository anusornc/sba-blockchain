(ns simple-test
  (:require [datomic-blockchain.datomic.connection :as conn]
            [datomic-blockchain.datomic.schema :as schema]
            [datomic-blockchain.config :as config]
            [datomic.api :as d]))

(defn -main []
  (let [c (conn/connect (config/load-config))]
    ;; Install schema first
    (println "Installing schema...")
    (schema/install-schema c)
    (println "Schema installed!")

    (let [tx-id (java.util.UUID/randomUUID)
          now (java.util.Date.)
          instant (.toInstant now)]
      (println "Creating test blockchain transaction...")
      (d/transact c
        [{:db/id "temp-tx"
          :blockchain/transaction tx-id
          :blockchain/timestamp instant
          :blockchain/hash "test-hash-001"
          :blockchain/previous-hash "00000000-0000-0000-0000-000000000000"
          :blockchain/nonce 12345
          :blockchain/creator (java.util.UUID/randomUUID)}])
      (println "Transaction created successfully!")
      (println "Verifying transaction...")
      (let [db (d/db c)
            txs (d/q '[:find ?tx :where [?tx :blockchain/transaction]] db)]
        (println "Found" (count txs) "transactions"))
      (System/exit 0))))
