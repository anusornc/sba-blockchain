(ns datomic-blockchain.datomic.query
  "Query functions for blockchain data"
  (:require [datomic.api :as d]))

(defn query-all-transactions
  "Query all transactions from the blockchain"
  [db]
  (d/q '[:find (pull ?tx [*])
         :where [?tx :blockchain/transaction]]
       db))

(defn query-transaction-by-id
  "Query a transaction by its ID"
  [db tx-id]
  (d/pull db '[*] tx-id))

(defn query-transactions-by-creator
  "Query all transactions by a specific creator"
  [db creator-id]
  (d/q '[:find (pull ?tx [*])
         :in $ ?creator
         :where [?tx :blockchain/creator ?creator]]
       db creator-id))

(defn query-blocks
  "Query all blocks"
  [db]
  (d/q '[:find (pull ?block [*])
         :where [?block :blockchain/index]]
       db))

(defn query-by-batch
  "Query traceability data by batch number"
  [db batch-num]
  (d/q '[:find (pull ?e [*])
         :in $ ?batch
         :where [?e :traceability/batch ?batch]]
       db batch-num))
