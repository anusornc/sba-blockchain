;; Cross-Chain Bridge Example
;; Demonstrates cross-chain transfer with PROV-O and Merkle proofs

(ns examples.cross-chain-bridge
  (:require [datomic-blockchain.bridge.core :as bridge]
            [datomic-blockchain.bridge.merkle :as merkle]))

;; Example 1: Merkle Proof Generation and Verification
(defn merkle-example []
  (println "\n=== Merkle Proof Example ===")
  (let [transactions [{:id 1 :data "tx1"}
                      {:id 2 :data "tx2"}
                      {:id 3 :data "tx3"}]
        tree (merkle/build-merkle-tree transactions)
        proof (merkle/generate-proof tree (merkle/hash-tx (first transactions)))]
    (println "Merkle root:" (subs (:root tree) 0 20) "...")
    (println "Proof verified:" (merkle/verify-proof 
                                 (:tx-hash proof) 
                                 (:proof-path proof)
                                 (:root tree)))))

;; Example 2: PROV-O Payload Creation
(defn prov-o-example []
  (println "\n=== PROV-O Payload Example ===")
  (let [entity {:prov/entity #uuid "550e8400-e29b-41d4-a716-446655440000"
                :prov/entity-type :product/chocolate-uht
                :traceability/batch "UHT-CHOC-2024-001"}
        activity {:prov/activity #uuid "550e8400-e29b-41d4-a716-446655440001"
                  :prov/activity-type :activity/transport}
        agent {:prov/agent #uuid "550e8400-e29b-41d4-a716-446655440002"
               :prov/agent-name "FreshCold Logistics"}
        
        payload (bridge/create-prov-o-payload entity activity agent)]
    (println "PROV-O payload created")
    (println "Valid?" (bridge/validate-prov-o-payload payload))))

;; Example 3: Bridge Configuration
(defn bridge-config-example []
  (println "\n=== Bridge Configuration Example ===")
  (let [config {:source-chain "thai-dairy-chain"
                :target-chain "global-supply-chain"
                :validators ["v1" "v2" "v3" "v4" "v5"]
                :threshold 3}]
    (println "Bridge config:" config)
    (println "Requires" (:threshold config) "of" (count (:validators config)) "signatures")))

;; Run all examples
(defn run-all []
  (merkle-example)
  (prov-o-example)
  (bridge-config-example)
  (println "\n=== Examples complete ==="))
