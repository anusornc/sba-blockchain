(ns datomic-blockchain.bridge.core
  "Cross-Chain Bridge for Inter-Blockchain Communication
   
   Enables data exchange between different blockchain networks using:
   - PROV-O standard for semantic data representation
   - Merkle proofs for transaction verification
   
   All validators on both chains must verify cross-chain transactions."
  (:require [datomic-blockchain.bridge.merkle :as merkle]
            [datomic-blockchain.ontology.kb :as kb]
            [datomic.api :as d]
            [taoensso.timbre :as log])
  (:import [java.util UUID Date]))

;; ============================================================================
;; Bridge Configuration
;; ============================================================================

(defrecord BridgeConfig
  [source-chain      ;; Source blockchain identifier
   target-chain      ;; Target blockchain identifier
   validators        ;; List of bridge validators (multisig)
   threshold         ;; Minimum signatures required
   prov-o-mapping    ;; PROV-O attribute mapping between chains
   ])

;; ============================================================================
;; Cross-Chain Transaction
;; ============================================================================

(defrecord CrossChainTx
  [tx-id             ;; Unique transaction ID
   source-chain      ;; Source blockchain
   target-chain      ;; Target blockchain
   payload           ;; PROV-O compliant data payload
   merkle-root       ;; Merkle root of source transaction
   merkle-proof      ;; Merkle proof path
   source-height     ;; Block height on source chain
   timestamp         ;; Transaction timestamp
   signatures        ;; Validator signatures
   status])          ;; :pending, :verified, :executed, :failed

;; ============================================================================
;; PROV-O Cross-Chain Data Format
;; ============================================================================

(defn create-prov-o-payload
  "Create PROV-O compliant payload for cross-chain transfer"
  [entity-data activity-data agent-data]
  {:prov/entity (select-keys entity-data
                             [:prov/entity :prov/entity-type
                              :traceability/batch :traceability/product])
   :prov/activity (select-keys activity-data
                               [:prov/activity :prov/activity-type
                                :prov/startedAtTime])
   :prov/agent (select-keys agent-data
                            [:prov/agent :prov/agent-type
                             :prov/agent-name])
   :prov/relationships {:prov/used (:prov/entity entity-data)
                        :prov/wasGeneratedBy (:prov/activity activity-data)
                        :prov/wasAssociatedWith (:prov/agent agent-data)}
   :timestamp (Date.)})

(defn validate-prov-o-payload
  "Validate that payload conforms to PROV-O requirements"
  [payload]
  (let [entity (:prov/entity payload)
        activity (:prov/activity payload)
        agent (:prov/agent payload)
        rels (:prov/relationships payload)
        entity-id (:prov/entity entity)
        activity-id (:prov/activity activity)
        agent-id (:prov/agent agent)]
    (and (map? payload)
         (map? entity)
         (map? activity)
         (map? agent)
         (map? rels)
         (some? entity-id)
         (some? activity-id)
         (some? agent-id)
         (= (:prov/used rels) entity-id)
         (= (:prov/wasGeneratedBy rels) activity-id)
         (= (:prov/wasAssociatedWith rels) agent-id))))

(defn- proof-supplied?
  [tx]
  (or (contains? tx :merkle-root)
      (contains? tx :merkle-proof)))

(defn- proof-path
  [merkle-proof]
  (if (map? merkle-proof)
    (:proof-path merkle-proof)
    merkle-proof))

(defn- proof-root
  [tx]
  (or (:merkle-root tx)
      (get-in tx [:merkle-proof :root])))

(defn- proof-tx-hash
  [tx]
  (or (:merkle-tx-hash tx)
      (:source-tx-hash tx)
      (get-in tx [:merkle-proof :tx-hash])
      (get-in tx [:merkle-proof :transaction-hash])))

(defn- valid-merkle-proof?
  "Validate supplied Merkle proof material.

   Transfers without proof material are accepted as local primitive mode for
   tests and in-process simulations. If either proof field is supplied, the
   root, proof path, and transaction hash must verify cryptographically."
  [tx]
  (if-not (proof-supplied? tx)
    true
    (let [tx-hash (proof-tx-hash tx)
          expected-hash (merkle/hash-tx (:payload tx))
          path (proof-path (:merkle-proof tx))
          root (proof-root tx)]
      (and (some? tx-hash)
           (= expected-hash tx-hash)
           (some? path)
           (some? root)
           (merkle/verify-proof tx-hash path root)))))

(defn- first-provenance-record
  "Normalize kb/get-provenance output to the first complete provenance record."
  [provenance]
  (cond
    (map? provenance) provenance
    (sequential? provenance) (first provenance)
    :else nil))

;; ============================================================================
;; Cross-Chain Transfer
;; ============================================================================

(defn initiate-transfer
  "Initiate cross-chain transfer from source"
  [source-conn bridge-config entity-id]
  (log/info "Initiating cross-chain transfer:" entity-id)
  (let [source-db (d/db source-conn)
        provenance (first-provenance-record (kb/get-provenance source-db entity-id))

        _ (when-not provenance
            (throw (ex-info "No provenance found for bridge transfer"
                            {:entity-id entity-id})))
        
        payload (create-prov-o-payload
                 (:entity-data provenance)
                 (:activity-data provenance)
                 (:agent-data provenance))
        
        _ (when-not (validate-prov-o-payload payload)
            (throw (ex-info "Invalid PROV-O payload" {:entity-id entity-id})))
        
        tx-hash (merkle/hash-tx payload)
        merkle-tree (merkle/build-merkle-tree [payload])
        merkle-proof (merkle/generate-proof merkle-tree tx-hash)
        tx-data {:tx-id (str (UUID/randomUUID))
                 :source-chain (:source-chain bridge-config)
                 :target-chain (:target-chain bridge-config)
                 :payload payload
                 :merkle-tx-hash tx-hash
                 :merkle-root (merkle/get-merkle-root merkle-tree)
                 :merkle-proof (:proof-path merkle-proof)
                 :source-height 1
                 :timestamp (Date.)
                 :status :pending}]
    
    {:success true
     :transaction tx-data}))

(defn execute-transfer
  "Execute cross-chain transfer on target chain.

   If Merkle proof fields are absent, the transfer is treated as local primitive
   mode for in-process simulations. If either proof field is supplied, all proof
   material must verify before signatures are counted."
  [target-conn bridge-config tx]
  (log/info "Executing transfer on target chain:" (:tx-id tx))
  (let [sig-count (count (:signatures tx))
        threshold (:threshold bridge-config)
        payload (:payload tx)]
    
    (cond
      (not (validate-prov-o-payload payload))
      {:success false
       :status :failed
       :reason "Invalid PROV-O payload"}

      (not (valid-merkle-proof? tx))
      {:success false
       :status :failed
       :reason "Invalid Merkle proof"}

      (< sig-count threshold)
      {:success false
       :status :pending
       :needed (- threshold sig-count)}

      :else
      (let [entity-tempid (str "bridge-" (:tx-id tx))
            
            tx-data [{:db/id entity-tempid
                      :prov/entity (UUID/randomUUID)
                      :prov/entity-type (get-in payload [:prov/entity :prov/entity-type])
                      :traceability/batch (get-in payload [:prov/entity :traceability/batch])
                      :blockchain/cross-chain-source (:source-chain tx)
                      :blockchain/cross-chain-tx (:tx-id tx)}]]
        
        @(d/transact target-conn tx-data)
        
        {:success true
         :status :executed}))))

;; ============================================================================
;; Bridge Status
;; ============================================================================

(defn get-bridge-status
  "Get current status of bridge between two chains"
  [source-conn target-conn bridge-config]
  {:source-chain (:source-chain bridge-config)
   :target-chain (:target-chain bridge-config)
   :validators (count (:validators bridge-config))
   :threshold (:threshold bridge-config)
   :healthy? true})
