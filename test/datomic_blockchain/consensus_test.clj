(ns datomic-blockchain.consensus-test
  "Unit tests for multi-consensus protocols"
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datomic-blockchain.consensus.protocol :as protocol]
            [datomic-blockchain.consensus.config :as config]))

;; ============================================================================
;; PoA Consensus Tests
;; ============================================================================

(deftest poa-consensus-creation
  (testing "PoA consensus creation"
    (let [validators ["v1" "v2" "v3"]
          poa (protocol/create-poa-consensus validators)]
      (is (= :poa (protocol/get-consensus-type poa)))
      (is (= validators (protocol/get-validators poa))))))

(deftest poa-validator-rotation
  (testing "PoA round-robin validator rotation"
    (let [validators ["v1" "v2" "v3"]
          poa (protocol/create-poa-consensus validators)]
      (is (= "v1" (protocol/select-validator poa 0)))
      (is (= "v2" (protocol/select-validator poa 1)))
      (is (= "v3" (protocol/select-validator poa 2)))
      (is (= "v1" (protocol/select-validator poa 3))) ;; Wrap around
      (is (= "v2" (protocol/select-validator poa 4))))))

(deftest poa-block-validation
  (testing "PoA block validation"
    (let [validators ["v1" "v2" "v3"]
          poa (protocol/create-poa-consensus validators)
          block {:blockchain/height 1
                 :blockchain/creator "v2"}
          prev-block {:blockchain/height 0}]
      (is (protocol/validate-block poa block prev-block validators))
      
      ;; Wrong validator for height
      (let [invalid-block {:blockchain/height 1
                           :blockchain/creator "v1"}]
        (is (not (protocol/validate-block poa invalid-block prev-block validators)))))))

(deftest poa-mining
  (testing "PoA block mining"
    (let [validators ["v1" "v2" "v3"]
          poa (protocol/create-poa-consensus validators)
          prev-block {:blockchain/height 0}
          transactions [{:tx "test"}]]
      ;; v2 mines block 1 (index 1)
      (let [block (protocol/mine-block poa transactions prev-block "v2")]
        (is (some? block))
        (is (= 1 (:blockchain/height block)))
        (is (= "v2" (:blockchain/creator block))))
      
      ;; v1 cannot mine block 1 (wrong validator)
      (let [block (protocol/mine-block poa transactions prev-block "v1")]
        (is (nil? block))))))

;; ============================================================================
;; PoS Consensus Tests
;; ============================================================================

(deftest pos-consensus-creation
  (testing "PoS consensus creation"
    (let [validators ["v1" "v2"]
          stakes {"v1" 1000 "v2" 2000}
          pos (protocol/create-pos-consensus validators stakes 500)]
      (is (= :pos (protocol/get-consensus-type pos)))
      (is (= validators (protocol/get-validators pos))))))

(deftest pos-stake-weighted-selection
  (testing "PoS stake-weighted validator selection"
    (let [validators ["v1" "v2" "v3"]
          stakes {"v1" 1000 "v2" 2000 "v3" 3000}  ;; 1:2:3 ratio
          pos (protocol/create-pos-consensus validators stakes 100)]
      ;; Selection is deterministic based on block height
      (is (string? (protocol/select-validator pos 0)))
      (is (string? (protocol/select-validator pos 1)))
      (is (contains? (set validators) (protocol/select-validator pos 10))))))

(deftest pos-minimum-stake
  (testing "PoS minimum stake requirement"
    (let [validators ["v1" "v2"]
          stakes {"v1" 5000 "v2" 100}  ;; v2 below min
          pos (protocol/create-pos-consensus validators stakes 1000)
          prev-block {:blockchain/height 0}
          transactions [{:tx "test"}]]
      ;; v1 can mine (above min)
      (is (some? (protocol/mine-block pos transactions prev-block "v1")))
      ;; v2 cannot mine (below min)
      (is (nil? (protocol/mine-block pos transactions prev-block "v2"))))))

(deftest pos-block-validation
  (testing "PoS block validation with stakes"
    (let [validators ["v1" "v2"]
          stakes {"v1" 5000 "v2" 2000}
          pos (protocol/create-pos-consensus validators stakes 1000)
          block {:blockchain/creator "v1"}]
      (is (protocol/validate-block pos block {} validators))
      
      ;; Unknown validator
      (let [invalid-block {:blockchain/creator "v3"}]
        (is (not (protocol/validate-block pos invalid-block {} validators)))))))

;; ============================================================================
;; PBFT Consensus Tests
;; ============================================================================

(deftest pbft-consensus-creation
  (testing "PBFT consensus creation"
    (let [validators ["v1" "v2" "v3" "v4"]
          pbft (protocol/create-pbft-consensus validators 1)]
      (is (= :pbft (protocol/get-consensus-type pbft)))
      (is (= validators (protocol/get-validators pbft))))))

(deftest pbft-validator-requirement
  (testing "PBFT requires at least 3f+1 validators"
    ;; f=1 requires 4 validators
    (is (some? (protocol/create-pbft-consensus ["v1" "v2" "v3" "v4"] 1)))
    
    ;; f=1 with only 3 should fail
    (is (thrown? Exception (protocol/create-pbft-consensus ["v1" "v2" "v3"] 1)))
    
    ;; f=2 requires 7 validators
    (is (some? (protocol/create-pbft-consensus ["v1" "v2" "v3" "v4" "v5" "v6" "v7"] 2)))
    (is (thrown? Exception (protocol/create-pbft-consensus ["v1" "v2" "v3" "v4" "v5" "v6"] 2)))))

(deftest pbft-primary-rotation
  (testing "PBFT primary (leader) rotation by view"
    (let [validators ["v1" "v2" "v3" "v4"]
          pbft (protocol/create-pbft-consensus validators 1)]
      ;; View 0: v1 is primary
      (is (= "v1" (protocol/select-validator pbft 0)))
      ;; View 1: v2 is primary (after 4 blocks)
      (is (= "v2" (protocol/select-validator pbft 4)))
      ;; View 2: v3 is primary
      (is (= "v3" (protocol/select-validator pbft 8)))
      ;; View 3: v4 is primary
      (is (= "v4" (protocol/select-validator pbft 12))))))

(deftest pbft-mining
  (testing "PBFT block mining (only primary proposes)"
    (let [validators ["v1" "v2" "v3" "v4"]
          pbft (protocol/create-pbft-consensus validators 1)
          prev-block {:blockchain/height 0 :blockchain/sequence 0}
          transactions [{:tx "test"}]]
      ;; v1 is primary for view 0
      (let [block (protocol/mine-block pbft transactions prev-block "v1")]
        (is (some? block))
        (is (= 1 (:blockchain/sequence block))))
      
      ;; v2 cannot propose (not primary)
      (is (nil? (protocol/mine-block pbft transactions prev-block "v2"))))))

;; ============================================================================
;; Consensus Manager Tests
;; ============================================================================

(deftest consensus-manager-creation
  (testing "Consensus manager creation from config"
    (let [poa-config {:type :poa :validators ["v1" "v2"]}
          manager (protocol/create-consensus-manager poa-config)]
      (is (= :poa (protocol/get-consensus-type manager)))
      (is (= ["v1" "v2"] (protocol/get-validators manager))))
    
    (let [pos-config {:type :pos :validators ["v1"] :stakes {"v1" 1000}}
          manager (protocol/create-consensus-manager pos-config)]
      (is (= :pos (protocol/get-consensus-type manager))))
    
    (let [pbft-config {:type :pbft :validators ["v1" "v2" "v3" "v4"] :f 1}
          manager (protocol/create-consensus-manager pbft-config)]
      (is (= :pbft (protocol/get-consensus-type manager))))))

;; ============================================================================
;; Network Consensus Validation Tests
;; ============================================================================

(deftest network-consensus-validation
  (testing "Network consensus type agreement"
    ;; Valid network - all nodes use PoA
    (let [network [{:consensus {:type :poa :validators ["v1" "v2"]}}
                   {:consensus {:type :poa :validators ["v1" "v2"]}}
                   {:consensus {:type :poa :validators ["v1" "v2"]}}]
          result (config/validate-network-consensus network)]
      (is (:valid? result))
      (is (= :poa (:consensus result))))
    
    ;; Invalid network - mixed consensus types
    (let [network [{:consensus {:type :poa :validators ["v1" "v2"]}}
                   {:consensus {:type :pos :validators ["v1" "v2"]}}]
          result (config/validate-network-consensus network)]
      (is (not (:valid? result)))
      (is (:error result)))
    
    ;; Invalid network - different validators
    (let [network [{:consensus {:type :poa :validators ["v1" "v2"]}}
                   {:consensus {:type :poa :validators ["v1" "v3"]}}]
          result (config/validate-network-consensus network)]
      (is (not (:valid? result))))))

;; ============================================================================
;; Configuration Validation Tests
;; ============================================================================

(deftest consensus-config-validation
  (testing "Configuration validation for each consensus type"
    ;; Valid PoA config
    (let [result (config/validate-consensus-config {:type :poa :validators ["v1"]})]
      (is (:valid? result)))
    
    ;; Valid PoS config
    (let [result (config/validate-consensus-config 
                   {:type :pos :validators ["v1"] :stakes {"v1" 1000}})]
      (is (:valid? result)))
    
    ;; Invalid PoS - missing stakes
    (let [result (config/validate-consensus-config 
                   {:type :pos :validators ["v1" "v2"] :stakes {"v1" 1000}})]
      (is (not (:valid? result))))
    
    ;; Valid PBFT config
    (let [result (config/validate-consensus-config 
                   {:type :pbft :validators ["v1" "v2" "v3" "v4"] :f 1})]
      (is (:valid? result)))
    
    ;; Invalid PBFT - insufficient validators
    (let [result (config/validate-consensus-config 
                   {:type :pbft :validators ["v1" "v2" "v3"] :f 1})]
      (is (not (:valid? result))))))
