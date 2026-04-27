;; Multi-Consensus Example
;; Demonstrates PoA, PoS, and PBFT consensus selection

(ns examples.multi-consensus
  (:require [datomic-blockchain.consensus.protocol :as consensus]
            [datomic-blockchain.consensus.config :as config]))

;; Example 1: PoA (Proof of Authority)
(defn poa-example []
  (println "\n=== PoA Consensus Example ===")
  (let [poa (consensus/create-poa-consensus 
              ["validator-corp-a" "validator-corp-b" "validator-corp-c"])]
    (println "Block 1 validator:" (consensus/select-validator poa 1))
    (println "Block 2 validator:" (consensus/select-validator poa 2))
    (println "Block 3 validator:" (consensus/select-validator poa 3))))

;; Example 2: PoS (Proof of Stake)
(defn pos-example []
  (println "\n=== PoS Consensus Example ===")
  (let [pos (consensus/create-pos-consensus 
              ["v1" "v2" "v3"]
              {"v1" 5000 "v2" 3000 "v3" 2000}
              1000)]
    (println "Block 10 validator:" (consensus/select-validator pos 10))
    (println "Block 20 validator:" (consensus/select-validator pos 20))))

;; Example 3: PBFT
(defn pbft-example []
  (println "\n=== PBFT Consensus Example ===")
  (let [pbft (consensus/create-pbft-consensus
               ["v-a" "v-b" "v-c" "v-d"] 1)]
    (println "View 0 primary:" (consensus/select-validator pbft 0))
    (println "View 1 primary:" (consensus/select-validator pbft 4))))

;; Run all examples
(defn run-all []
  (poa-example)
  (pos-example)
  (pbft-example)
  (println "\n=== Examples complete ==="))
