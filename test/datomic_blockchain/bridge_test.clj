(ns datomic-blockchain.bridge-test
  "Unit tests for cross-chain bridge with Merkle proofs"
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datomic-blockchain.bridge.core :as bridge]
            [datomic-blockchain.bridge.merkle :as merkle]))

;; ============================================================================
;; Merkle Tree Tests
;; ============================================================================

(deftest merkle-tree-creation
  (testing "Merkle tree building from transactions"
    (let [transactions [{:id 1 :data "tx1"}
                        {:id 2 :data "tx2"}
                        {:id 3 :data "tx3"}]
          tree (merkle/build-merkle-tree transactions)]
      (is (some? (:root tree)))
      (is (= 3 (:leaf-count tree)))
      (is (number? (:tree-height tree)))
      (is (= 64 (count (:root tree))))))) ;; SHA-256 hex length

(deftest merkle-tree-empty
  (testing "Merkle tree with single transaction"
    (let [transactions [{:id 1}]
          tree (merkle/build-merkle-tree transactions)]
      (is (some? (:root tree)))
      (is (= 1 (:leaf-count tree))))))

(deftest merkle-tree-power-of-2
  (testing "Merkle tree with power of 2 transactions"
    (let [transactions [{:id 1} {:id 2} {:id 3} {:id 4}]
          tree (merkle/build-merkle-tree transactions)]
      (is (= 4 (:leaf-count tree)))
      (is (= 2 (:tree-height tree))))))

;; ============================================================================
;; Merkle Proof Tests
;; ============================================================================

(deftest merkle-proof-generation
  (testing "Merkle proof generation for transaction"
    (let [transactions [{:id 1 :data "tx1"} {:id 2 :data "tx2"}]
          tree (merkle/build-merkle-tree transactions)
          tx-hash (merkle/hash-tx (first transactions))
          proof (merkle/generate-proof tree tx-hash)]
      (is (some? proof))
      (is (= tx-hash (:tx-hash proof)))
      (is (= (:root tree) (:root proof))))))

(deftest merkle-proof-verification
  (testing "Merkle proof verification for a single-leaf tree"
    (let [transactions [{:id 1}]
          tree (merkle/build-merkle-tree transactions)
          tx-hash (merkle/hash-tx (first transactions))
          proof (merkle/generate-proof tree tx-hash)]
      (is (empty? (:proof-path proof)))
      (is (merkle/verify-proof tx-hash (:proof-path proof) (:root tree))))))

(deftest merkle-proof-verification-multi-leaf
  (testing "Merkle proof verification for a multi-leaf tree"
    (let [transactions [{:id 1 :data "tx1"}
                        {:id 2 :data "tx2"}
                        {:id 3 :data "tx3"}
                        {:id 4 :data "tx4"}]
          tree (merkle/build-merkle-tree transactions)
          tx-hash (merkle/hash-tx (nth transactions 2))
          proof (merkle/generate-proof tree tx-hash)]
      (is (= [{:hash (merkle/hash-tx (nth transactions 3)) :direction :right}
              {:hash (merkle/hash-pair (merkle/hash-tx (nth transactions 0))
                                       (merkle/hash-tx (nth transactions 1)))
               :direction :left}]
             (:proof-path proof)))
      (is (= [:right :left] (:directions proof)))
      (is (merkle/verify-proof tx-hash (:proof-path proof) (:root tree))))))

(deftest merkle-invalid-proof
  (testing "Merkle proof verification fails for invalid data"
    (let [transactions [{:id 1} {:id 2}]
          tree (merkle/build-merkle-tree transactions)
          tx-hash (merkle/hash-tx (first transactions))
          proof (merkle/generate-proof tree tx-hash)
          wrong-hash (merkle/hash-tx {:id 999})
          tampered-proof (assoc-in (:proof-path proof) [0 :hash] (merkle/sha-256 "dummy"))]
      (is (false? (merkle/verify-proof wrong-hash (:proof-path proof) (:root tree))))
      (is (false? (merkle/verify-proof tx-hash tampered-proof (:root tree))))
      (is (false? (merkle/verify-proof tx-hash [(merkle/sha-256 "legacy-no-direction")] (:root tree)))))))

(deftest merkle-batch-verification
  (testing "Batch verification uses a distinct proof per transaction"
    (let [transactions [{:id 1 :data "tx1"}
                        {:id 2 :data "tx2"}
                        {:id 3 :data "tx3"}]
          tree (merkle/build-merkle-tree transactions)
          tx-hashes (mapv merkle/hash-tx transactions)
          proofs (mapv #(merkle/generate-proof tree %) tx-hashes)
          proof-paths (mapv :proof-path proofs)
          invalid-proof-paths (assoc-in proof-paths [1 0 :direction] :right)]
      (is (merkle/verify-batch (:root tree) tx-hashes proof-paths))
      (is (false? (merkle/verify-batch (:root tree) tx-hashes invalid-proof-paths)))
      (is (false? (merkle/verify-batch (:root tree) tx-hashes (pop proof-paths)))))))

;; ============================================================================
;; SHA-256 Hash Tests
;; ============================================================================

(deftest sha256-hashing
  (testing "SHA-256 hash computation"
    (let [hash1 (merkle/sha-256 "test")
          hash2 (merkle/sha-256 "test")
          hash3 (merkle/sha-256 "different")]
      ;; Same input produces same hash
      (is (= hash1 hash2))
      ;; Different input produces different hash
      (is (not= hash1 hash3))
      ;; Correct length (64 hex chars = 256 bits)
      (is (= 64 (count hash1))))))

(deftest transaction-hashing
  (testing "Transaction hashing"
    (let [tx {:id 1 :data "test"}
          hash (merkle/hash-tx tx)]
      (is (string? hash))
      (is (= 64 (count hash))))))

(deftest pair-hashing
  (testing "Pair hashing for Merkle tree"
    (let [left (merkle/sha-256 "left")
          right (merkle/sha-256 "right")
          parent (merkle/hash-pair left right)]
      (is (= 64 (count parent)))
      ;; Order matters
      (is (not= parent (merkle/hash-pair right left))))))

;; ============================================================================
;; PROV-O Payload Tests
;; ============================================================================

(deftest prov-o-payload-creation
  (testing "PROV-O compliant payload creation"
    (let [entity {:prov/entity #uuid "550e8400-e29b-41d4-a716-446655440000"
                  :prov/entity-type :product/test
                  :traceability/batch "BATCH-001"}
          activity {:prov/activity #uuid "550e8400-e29b-41d4-a716-446655440001"
                    :prov/activity-type :activity/test}
          agent {:prov/agent #uuid "550e8400-e29b-41d4-a716-446655440002"
                 :prov/agent-name "Test Agent"}
          
          payload (bridge/create-prov-o-payload entity activity agent)]
      
      (is (some? (:prov/entity payload)))
      (is (some? (:prov/activity payload)))
      (is (some? (:prov/agent payload)))
      (is (some? (:prov/relationships payload)))
      (is (some? (:timestamp payload))))))

(deftest prov-o-payload-validation
  (testing "PROV-O payload validation"
    ;; Valid payload
    (let [valid-payload {:prov/entity {:prov/entity #uuid "550e8400-e29b-41d4-a716-446655440000"}
                         :prov/activity {:prov/activity #uuid "550e8400-e29b-41d4-a716-446655440001"}
                         :prov/agent {:prov/agent #uuid "550e8400-e29b-41d4-a716-446655440002"}
                         :prov/relationships {:prov/used #uuid "550e8400-e29b-41d4-a716-446655440000"
                                              :prov/wasGeneratedBy #uuid "550e8400-e29b-41d4-a716-446655440001"
                                              :prov/wasAssociatedWith #uuid "550e8400-e29b-41d4-a716-446655440002"}}]
      (is (bridge/validate-prov-o-payload valid-payload)))
    
    ;; Invalid - missing entity
    (let [invalid-payload {:prov/activity {:prov/activity #uuid "550e8400-e29b-41d4-a716-446655440001"}}]
      (is (not (bridge/validate-prov-o-payload invalid-payload))))
    
    ;; Invalid - missing entity ID
    (let [invalid-payload {:prov/entity {:other-key "value"}
                           :prov/activity {:prov/activity #uuid "550e8400-e29b-41d4-a716-446655440001"}
                           :prov/agent {:prov/agent #uuid "550e8400-e29b-41d4-a716-446655440002"}
                           :prov/relationships {:prov/used nil
                                                :prov/wasGeneratedBy #uuid "550e8400-e29b-41d4-a716-446655440001"
                                                :prov/wasAssociatedWith #uuid "550e8400-e29b-41d4-a716-446655440002"}}]
      (is (not (bridge/validate-prov-o-payload invalid-payload))))

    ;; Invalid - relationship points at a different activity
    (let [invalid-payload {:prov/entity {:prov/entity #uuid "550e8400-e29b-41d4-a716-446655440000"}
                           :prov/activity {:prov/activity #uuid "550e8400-e29b-41d4-a716-446655440001"}
                           :prov/agent {:prov/agent #uuid "550e8400-e29b-41d4-a716-446655440002"}
                           :prov/relationships {:prov/used #uuid "550e8400-e29b-41d4-a716-446655440000"
                                                :prov/wasGeneratedBy #uuid "650e8400-e29b-41d4-a716-446655440001"
                                                :prov/wasAssociatedWith #uuid "550e8400-e29b-41d4-a716-446655440002"}}]
      (is (not (bridge/validate-prov-o-payload invalid-payload))))))

;; ============================================================================
;; Bridge Configuration Tests
;; ============================================================================

(deftest bridge-config-creation
  (testing "Bridge configuration creation"
    (let [config {:source-chain "chain-a"
                  :target-chain "chain-b"
                  :validators ["v1" "v2" "v3"]
                  :threshold 2}]
      (is (= "chain-a" (:source-chain config)))
      (is (= "chain-b" (:target-chain config)))
      (is (= 3 (count (:validators config))))
      (is (= 2 (:threshold config))))))

;; ============================================================================
;; Bridge Status Tests
;; ============================================================================

(deftest bridge-status
  (testing "Bridge status reporting"
    (let [config {:source-chain "chain-1"
                  :target-chain "chain-2"
                  :validators ["v1" "v2" "v3"]
                  :threshold 2}
          ;; Mock connections (nil for test)
          status (bridge/get-bridge-status nil nil config)]
      (is (= "chain-1" (:source-chain status)))
      (is (= "chain-2" (:target-chain status)))
      (is (= 3 (:validators status)))
      (is (= 2 (:threshold status)))
      (is (:healthy? status)))))

;; ============================================================================
;; Integration Test Helpers
;; ============================================================================

(deftest cross-chain-flow-simulation
  (testing "Simulated cross-chain transfer flow"
    ;; This simulates the flow without actual Datomic connections
    (let [;; 1. Setup bridge config
          bridge-config {:source-chain "source"
                         :target-chain "target"
                         :validators ["v1" "v2" "v3"]
                         :threshold 2}
          
          ;; 2. Create PROV-O payload
          entity {:prov/entity #uuid "550e8400-e29b-41d4-a716-446655440000"
                  :prov/entity-type :product/chocolate-uht
                  :traceability/batch "UHT-CHOC-2024-001"}
          activity {:prov/activity #uuid "550e8400-e29b-41d4-a716-446655440001"
                    :prov/activity-type :activity/transport}
          agent {:prov/agent #uuid "550e8400-e29b-41d4-a716-446655440002"
                 :prov/agent-name "FreshCold Logistics"}
          
          payload (bridge/create-prov-o-payload entity activity agent)]
      
      ;; 3. Validate PROV-O compliance
      (is (bridge/validate-prov-o-payload payload))
      
      ;; 4. Simulate Merkle proof
      (let [transactions [{:payload payload}]
            tree (merkle/build-merkle-tree transactions)
            proof (merkle/generate-proof tree (merkle/hash-tx (first transactions)))]
        
        ;; 5. Verify proof
        (is (merkle/verify-proof (:tx-hash proof) 
                                 (:proof-path proof) 
                                 (:root tree)))))))
