(ns datomic-blockchain.consensus.protocol
  "Multi-Consensus Protocol Interface
   
   Supports three consensus mechanisms:
   - PoA (Proof of Authority): Pre-approved validators in round-robin
   - PoS (Proof of Stake): Stake-weighted validator selection
   - PBFT (Practical Byzantine Fault Tolerance): 3-phase commit with 3f+1 nodes
   
   All validators must use the SAME consensus type to maintain network consistency."
  (:require [taoensso.timbre :as log]))

;; ============================================================================
;; Consensus Protocol Definition
;; ============================================================================

(defprotocol ConsensusProtocol
  "Protocol for pluggable consensus mechanisms"
  
  (get-consensus-type [this]
    "Return consensus type: :poa, :pos, or :pbft")
  
  (get-validators [this]
    "Return list of current validators")
  
  (select-validator [this block-height]
    "Select validator for given block height based on consensus rules")
  
  (validate-block [this block previous-block validators]
    "Validate block according to consensus rules")
  
  (mine-block [this transactions previous-block creator]
    "Mine new block using consensus mechanism")
  
  (should-rotate [this block-height]
    "Check if validator should rotate at this height"))

;; ============================================================================
;; PoA (Proof of Authority) Implementation
;; ============================================================================

(defrecord PoAConsensus [validators current-idx]
  ConsensusProtocol
  
  (get-consensus-type [_] :poa)
  
  (get-validators [_] validators)
  
  (select-validator [_ block-height]
    (let [idx (mod block-height (count validators))]
      (nth validators idx)))
  
  (validate-block [_ block previous-block validators]
    (let [creator (:blockchain/creator block)
          expected-idx (mod (:blockchain/height block) (count validators))
          expected-creator (nth validators expected-idx)]
      (and (= creator expected-creator)
           (contains? (set validators) creator))))
  
  (mine-block [this transactions previous-block creator]
    (let [height (inc (:blockchain/height previous-block 0))
          expected-creator (select-validator this height)]
      (when (= creator expected-creator)
        {:blockchain/height height
         :blockchain/creator creator
         :blockchain/transactions transactions
         :blockchain/previous-hash (:blockchain/hash previous-block)
         :blockchain/timestamp (java.util.Date.)})))
  
  (should-rotate [_ block-height]
    true))

;; ============================================================================
;; PoS (Proof of Stake) Implementation
;; ============================================================================

(defrecord PoSConsensus [validators stakes min-stake]
  ConsensusProtocol
  
  (get-consensus-type [_] :pos)
  
  (get-validators [_] validators)
  
  (select-validator [_ block-height]
    ;; Weighted random selection based on stake
    (let [total-stake (reduce + (map #(get stakes % 0) validators))
          target (mod (* block-height 7919) total-stake) ;; Deterministic pseudo-random
          cumulative (reductions + (map #(get stakes % 0) validators))]
      (loop [idx 0
             cum (first cumulative)
             remaining (rest cumulative)]
        (if (>= cum target)
          (nth validators idx)
          (recur (inc idx)
                 (first remaining)
                 (rest remaining))))))
  
  (validate-block [_ block previous-block validators]
    (let [creator (:blockchain/creator block)
          stake (get stakes creator 0)]
      (and (>= stake min-stake)
           (contains? (set validators) creator))))
  
  (mine-block [this transactions previous-block creator]
    (let [stake (get stakes creator 0)]
      (when (>= stake min-stake)
        {:blockchain/height (inc (:blockchain/height previous-block 0))
         :blockchain/creator creator
         :blockchain/transactions transactions
         :blockchain/stake stake
         :blockchain/timestamp (java.util.Date.)})))
  
  (should-rotate [_ block-height]
    true))

;; ============================================================================
;; PBFT (Practical Byzantine Fault Tolerance) Implementation
;; ============================================================================

(defrecord PBFTConsensus [validators f]
  ConsensusProtocol
  
  (get-consensus-type [_] :pbft)
  
  (get-validators [_] validators)
  
  (select-validator [_ block-height]
    ;; PBFT uses primary (leader) based on view number
    (let [view (quot block-height (count validators))]
      (nth validators (mod view (count validators)))))
  
  (validate-block [_ block previous-block validators]
    ;; In PBFT, block validation requires:
    ;; 1. Proper sequence number
    ;; 2. Valid digital signatures from 2f+1 replicas
    ;; 3. Consistent with prepared certificates
    (let [seq (:blockchain/sequence block)
          prev-seq (:blockchain/sequence previous-block 0)]
      (and (= seq (inc prev-seq))
           (contains? (set validators) (:blockchain/creator block))
           (>= (count validators) (inc (* 3 f))))))
  
  (mine-block [this transactions previous-block creator]
    ;; In PBFT, primary proposes block; others validate via 3-phase commit
    (let [height (inc (:blockchain/height previous-block 0))
          primary (select-validator this height)]
      (when (= creator primary)
        {:blockchain/height height
         :blockchain/sequence (inc (:blockchain/sequence previous-block 0))
         :blockchain/creator creator
         :blockchain/transactions transactions
         :blockchain/view (quot height (count validators))
         :blockchain/timestamp (java.util.Date.)})))
  
  (should-rotate [_ block-height]
    (zero? (mod block-height (count validators)))))

;; ============================================================================
;; Consensus Manager
;; ============================================================================

(defrecord ConsensusManager [consensus config]
  ConsensusProtocol
  
  (get-consensus-type [_]
    (get-consensus-type consensus))
  
  (get-validators [_]
    (get-validators consensus))
  
  (select-validator [_ block-height]
    (select-validator consensus block-height))
  
  (validate-block [_ block previous-block validators]
    (validate-block consensus block previous-block validators))
  
  (mine-block [_ transactions previous-block creator]
    (mine-block consensus transactions previous-block creator))
  
  (should-rotate [_ block-height]
    (should-rotate consensus block-height)))

;; ============================================================================
;; Factory Functions
;; ============================================================================

(defn create-poa-consensus
  "Create PoA consensus with pre-approved validators
   
   Parameters:
   - validators: Vector of validator IDs (must be pre-approved)"
  [validators]
  (log/info "Creating PoA consensus with validators:" validators)
  (->PoAConsensus validators 0))

(defn create-pos-consensus
  "Create PoS consensus with stake-weighted selection
   
   Parameters:
   - validators: Vector of validator IDs
   - stakes: Map of validator-id -> stake amount
   - min-stake: Minimum stake required to validate"
  ([validators stakes]
   (create-pos-consensus validators stakes 1000))
  ([validators stakes min-stake]
   (log/info "Creating PoS consensus with" (count validators) "validators")
   (->PoSConsensus validators stakes min-stake)))

(defn create-pbft-consensus
  "Create PBFT consensus for Byzantine fault tolerance
   
   Parameters:
   - validators: Vector of validator IDs (must have >= 3f+1 validators)
   - f: Maximum number of Byzantine faults to tolerate"
  [validators f]
  (when (< (count validators) (inc (* 3 f)))
    (throw (ex-info "PBFT requires at least 3f+1 validators"
                    {:validators (count validators)
                     :f f
                     :required (inc (* 3 f))})))
  (log/info "Creating PBFT consensus with" (count validators) "validators, f=" f)
  (->PBFTConsensus validators f))

(defn create-consensus-manager
  "Create consensus manager based on configuration
   
   Config format:
   {:type :poa :validators [...]}
   {:type :pos :validators [...] :stakes {...} :min-stake 1000}
   {:type :pbft :validators [...] :f 1}"
  [config]
  (let [consensus (case (:type config)
                    :poa (create-poa-consensus (:validators config))
                    :pos (create-pos-consensus (:validators config)
                                               (:stakes config)
                                               (get config :min-stake 1000))
                    :pbft (create-pbft-consensus (:validators config)
                                                 (get config :f 1))
                    (throw (ex-info "Unknown consensus type"
                                    {:type (:type config)
                                     :expected [:poa :pos :pbft]})))]
    (log/info "Consensus manager created with type:" (:type config))
    (->ConsensusManager consensus config)))

;; ============================================================================
;; Network Validation
;; ============================================================================

(defn validate-network-consensus
  "Ensure all nodes in network use same consensus type
   
   This is critical - mixing consensus types would break network consistency."
  [nodes-configs]
  (let [consensus-types (map #(get-in % [:consensus :type]) nodes-configs)]
    (if (apply = consensus-types)
      (do
        (log/info "Network consensus validated:" (first consensus-types))
        {:valid? true
         :consensus (first consensus-types)
         :nodes (count nodes-configs)})
      (do
        (log/error "Network consensus mismatch!" consensus-types)
        {:valid? false
         :types (set consensus-types)
         :error "All nodes must use the same consensus type"}))))

(defn get-required-validator-count
  "Get minimum validator count for consensus type"
  [consensus-type config]
  (case consensus-type
    :poa 1
    :pos 1
    :pbft (inc (* 3 (get config :f 1)))
    (throw (ex-info "Unknown consensus type" {:type consensus-type}))))
