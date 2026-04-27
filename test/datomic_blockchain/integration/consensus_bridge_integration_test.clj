(ns datomic-blockchain.integration.consensus-bridge-integration-test
  "Integration tests for consensus and bridge working together"
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datomic-blockchain.consensus.protocol :as consensus]
            [datomic-blockchain.consensus.config :as config]
            [datomic-blockchain.bridge.core :as bridge]
            [datomic-blockchain.bridge.merkle :as merkle]))

;; ============================================================================
;; Consensus + Bridge Integration Tests
;; ============================================================================

(deftest consensus-with-cross-chain-transaction
  (testing "Cross-chain transaction validated by consensus"
    (let [;; Setup PoA consensus
          validators ["v1" "v2" "v3"]
          poa (consensus/create-poa-consensus validators)
          
          ;; Create cross-chain bridge config
          bridge-config {:source-chain "chain-a"
                         :target-chain "chain-b"
                         :validators validators
                         :threshold 2}
          
          ;; Create PROV-O payload
          entity {:prov/entity #uuid "550e8400-e29b-41d4-a716-446655440000"
                  :prov/entity-type :product/chocolate-uht
                  :traceability/batch "UHT-CHOC-2024-001"}
          activity {:prov/activity #uuid "550e8400-e29b-41d4-a716-446655440001"
                    :prov/activity-type :activity/transport}
          agent {:prov/agent #uuid "550e8400-e29b-41d4-a716-446655440002"
                 :prov/agent-name "FreshCold Logistics"}
          
          payload (bridge/create-prov-o-payload entity activity agent)]
      
      ;; Validate PROV-O payload
      (is (bridge/validate-prov-o-payload payload))
      
      ;; Create Merkle proof
      (let [transactions [{:payload payload :creator "v1"}]
            tree (merkle/build-merkle-tree transactions)
            proof (merkle/generate-proof tree (merkle/hash-tx (first transactions)))]
        
        ;; Verify proof
        (is (merkle/verify-proof (:tx-hash proof) 
                                 (:proof-path proof) 
                                 (:root tree)))
        
        ;; Consensus: v2 is selected for block 1 (round-robin: mod 1 3 = 1 -> "v2")
        (is (= "v2" (consensus/select-validator poa 1)))))))

(deftest pos-consensus-with-bridge-staking
  (testing "PoS consensus with bridge validators staking"
    (let [;; Setup PoS consensus with validators as bridge operators
          validators ["bridge-v1" "bridge-v2" "bridge-v3"]
          stakes {"bridge-v1" 10000  ;; High stake = more likely to validate
                  "bridge-v2" 5000
                  "bridge-v3" 3000}
          pos (consensus/create-pos-consensus validators stakes 1000)
          
          ;; Bridge requires 2 of 3 signatures
          bridge-config {:source-chain "enterprise-chain"
                         :target-chain "partner-chain"
                         :validators validators
                         :threshold 2}]
      
      ;; All validators have sufficient stake
      (is (every? #(>= (get stakes %) 1000) validators))
      
      ;; Validator selection is stake-weighted
      (let [selected (consensus/select-validator pos 10)]
        (is (contains? (set validators) selected))))))

(deftest pbft-consensus-for-bridge-security
  (testing "PBFT consensus for secure cross-chain bridge"
    (let [;; Setup PBFT with 4 validators (tolerates 1 Byzantine fault)
          validators ["secure-v1" "secure-v2" "secure-v3" "secure-v4"]
          pbft (consensus/create-pbft-consensus validators 1)
          
          ;; High-security bridge with same validators
          bridge-config {:source-chain "high-value-chain"
                         :target-chain "settlement-chain"
                         :validators validators
                         :threshold 3}  ;; 3 of 4 needed
          
          ;; Simulate cross-chain transaction
          payload {:prov/entity {:prov/entity #uuid "550e8400-e29b-41d4-a716-446655440000"
                                 :traceability/batch "HIGH-VALUE-001"}
                   :value 1000000}]  ;; High value transaction
      
      ;; PBFT can tolerate 1 faulty validator
      (is (= 1 (:f pbft)))
      (is (= 4 (count validators)))
      
      ;; Primary rotates
      (let [primary-0 (consensus/select-validator pbft 0)
            primary-1 (consensus/select-validator pbft 4)]
        (is (not= primary-0 primary-1))))))

(deftest network-consensus-agreement-for-bridge
  (testing "All bridge nodes must agree on consensus"
    (let [;; Simulate network of bridge nodes
          network-nodes [{:node "bridge-node-1"
                          :consensus {:type :poa
                                      :validators ["v1" "v2" "v3"]}}
                         {:node "bridge-node-2"
                          :consensus {:type :poa
                                      :validators ["v1" "v2" "v3"]}}
                         {:node "bridge-node-3"
                          :consensus {:type :poa
                                      :validators ["v1" "v2" "v3"]}}]
          
          ;; Validate network consensus
          validation (config/validate-network-consensus network-nodes)]
      
      ;; All nodes should agree
      (is (:valid? validation))
      (is (= :poa (:consensus validation)))
      (is (= 3 (:nodes validation))))))

(deftest cross-chain-with-consensus-validation
  (testing "Full flow: Cross-chain tx with consensus validation"
    (let [;; Setup
          validators ["validator-1" "validator-2" "validator-3"]
          poa (consensus/create-poa-consensus validators)
          
          ;; Create bridge config
          bridge-config {:source-chain "farm-chain"
                         :target-chain "factory-chain"
                         :validators validators
                         :threshold 2}
          
          ;; Step 1: Create PROV-O payload
          entity {:prov/entity #uuid "550e8400-e29b-41d4-a716-446655440000"
                  :prov/entity-type :product/raw-milk
                  :traceability/batch "MILK-THAI-2024-001"}
          activity {:prov/activity #uuid "550e8400-e29b-41d4-a716-446655440001"
                    :prov/activity-type :activity/milking}
          agent {:prov/agent #uuid "550e8400-e29b-41d4-a716-446655440002"
                 :prov/agent-name "Happy Dairy Farm"}
          
          payload (bridge/create-prov-o-payload entity activity agent)]
      
      ;; Step 2: Validate PROV-O
      (is (bridge/validate-prov-o-payload payload))
      
      ;; Step 3: Create transaction with consensus
      (let [creator (consensus/select-validator poa 1)  ;; First validator
            prev-block {:blockchain/height 0}
            transactions [{:type :cross-chain
                          :payload payload
                          :bridge bridge-config}]
            
            block (consensus/mine-block poa transactions prev-block creator)]
        
        ;; Step 4: Block should be valid
        (is (some? block))
        (is (= 1 (:blockchain/height block)))
        
        ;; Step 5: Generate Merkle proof
        (let [tree (merkle/build-merkle-tree transactions)
              proof (merkle/generate-proof tree (merkle/hash-tx (first transactions)))]
          
          ;; Step 6: Verify proof
          (is (merkle/verify-proof (:tx-hash proof)
                                   (:proof-path proof)
                                   (:root tree))))))))

;; ============================================================================
;; Multi-Chain Scenario Tests
;; ============================================================================

(deftest multi-chain-supply-chain
  (testing "Supply chain spanning multiple chains with consensus"
    (let [;; Farm chain uses PoA
          farm-validators ["farm-node-1" "farm-node-2"]
          farm-consensus (consensus/create-poa-consensus farm-validators)
          
          ;; Factory chain uses PoS
          factory-validators ["factory-node-1" "factory-node-2" "factory-node-3"]
          factory-stakes {"factory-node-1" 5000
                         "factory-node-2" 3000
                         "factory-node-3" 2000}
          factory-consensus (consensus/create-pos-consensus 
                              factory-validators factory-stakes 1000)
          
          ;; Bridge between chains
          farm-to-factory-bridge {:source-chain "farm-chain"
                                  :target-chain "factory-chain"
                                  :validators ["bridge-1" "bridge-2"]
                                  :threshold 2}]
      
      ;; Farm creates product
      (let [farm-creator (consensus/select-validator farm-consensus 1)
            farm-block (consensus/mine-block 
                         farm-consensus
                         [{:product "raw-milk" :batch "001"}]
                         {:blockchain/height 0}
                         farm-creator)]
        (is (some? farm-block)))
      
      ;; Factory processes with PoS
      (let [factory-creator (consensus/select-validator factory-consensus 10)
            factory-block (consensus/mine-block
                            factory-consensus
                            [{:product "uht-milk" :source-batch "001"}]
                            {:blockchain/height 0}
                            factory-creator)]
        (is (some? factory-block))))))

;; ============================================================================
;; Consensus Configuration Persistence Tests
;; ============================================================================

(deftest consensus-config-save-and-load
  (testing "Consensus configuration can be persisted and restored"
    (let [;; Create config
          config {:type :poa
                  :validators ["v1" "v2" "v3"]}
          
          ;; Save to temp file
          temp-file "/tmp/test-consensus-config.edn"
          _ (config/save-consensus-config config temp-file)
          
          ;; Load back
          loaded (config/load-consensus-from-file temp-file)]
      
      ;; Should be identical
      (is (= config loaded))
      
      ;; Cleanup
      (clojure.java.io/delete-file temp-file))))
