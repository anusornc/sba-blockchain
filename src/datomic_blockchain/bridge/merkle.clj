(ns datomic-blockchain.bridge.merkle
  "Merkle Tree Implementation for Cross-Chain Verification
   
   Provides cryptographic proofs that transactions exist in a block.
   Used for cross-chain bridge verification."
  (:require [taoensso.timbre :as log])
  (:import [java.security MessageDigest]
           [java.nio.charset StandardCharsets]))

;; ============================================================================
;; Hash Functions
;; ============================================================================

(defn sha-256
  "Compute SHA-256 hash of input"
  [input]
  (let [digest (MessageDigest/getInstance "SHA-256")
        bytes (.getBytes (str input) StandardCharsets/UTF_8)]
    (->> (.digest digest bytes)
         (map (fn [b] (format "%02x" (bit-and b 0xff))))
         (apply str))))

(defn hash-tx
  "Hash a transaction for Merkle tree
   
   Uses transaction data serialized to string."
  [tx-data]
  (sha-256 (str tx-data)))

(defn hash-pair
  "Hash two nodes together"
  [left right]
  (sha-256 (str left right)))

(defn- sha-256-hex?
  [value]
  (and (string? value)
       (boolean (re-matches #"[0-9a-f]{64}" value))))

(defn- next-level
  "Build the next Merkle level, promoting an odd final node unchanged."
  [level]
  (mapv (fn [[left right]]
          (if right
            (hash-pair left right)
            left))
        (partition-all 2 level)))

(defn- build-levels
  [leaves]
  (when (seq leaves)
    (loop [levels [(vec leaves)]]
      (let [current-level (peek levels)]
        (if (= 1 (count current-level))
          levels
          (recur (conj levels (next-level current-level))))))))

(defn- proof-step
  [level index]
  (let [sibling-index (if (even? index)
                        (inc index)
                        (dec index))
        sibling-hash (get level sibling-index)]
    (when sibling-hash
      {:hash sibling-hash
       :direction (if (even? index) :right :left)})))

(defn- proof-path
  [levels leaf-index]
  (loop [remaining-levels (butlast levels)
         index leaf-index
         path []]
    (if (empty? remaining-levels)
      path
      (let [level (first remaining-levels)]
        (recur (rest remaining-levels)
               (quot index 2)
               (if-let [step (proof-step level index)]
                 (conj path step)
                 path))))))

(defn- valid-proof-step?
  [step]
  (and (map? step)
       (sha-256-hex? (:hash step))
       (contains? #{:left :right "left" "right"} (:direction step))))

(defn- normalize-direction
  [direction]
  (case direction
    :left :left
    "left" :left
    :right :right
    "right" :right
    nil))

;; ============================================================================
;; Merkle Tree Construction
;; ============================================================================

(defn build-merkle-tree
  "Build Merkle tree from list of transactions
   
   Returns tree as nested structure with :root hash."
  [transactions]
  (let [leaves (mapv hash-tx transactions)
        levels (build-levels leaves)]
    {:root (some-> levels peek first)
     :leaf-count (count leaves)
     :tree-height (max 0 (dec (count levels)))
     :leaves leaves
     :levels levels}))

;; ============================================================================
;; Merkle Proof Generation
;; ============================================================================

(declare verify-proof)

(defn generate-proof
  "Generate Merkle proof for a specific transaction
   
   Returns sibling hashes with direction metadata needed to verify inclusion."
  [merkle-tree tx-hash]
  (when (sha-256-hex? tx-hash)
    (log/info "Generating Merkle proof for:" (subs tx-hash 0 16) "..."))
  (let [leaf-index (first (keep-indexed (fn [idx leaf]
                                          (when (= tx-hash leaf) idx))
                                        (:leaves merkle-tree)))
        path (if (some? leaf-index)
               (proof-path (:levels merkle-tree) leaf-index)
               [])]
    {:tx-hash tx-hash
     :root (:root merkle-tree)
     :proof-path path
     :directions (mapv :direction path)
     :leaf-index leaf-index
     :verified (and (some? leaf-index)
                    (verify-proof tx-hash path (:root merkle-tree)))}))

;; ============================================================================
;; Merkle Proof Verification
;; ============================================================================

(defn verify-proof
  "Verify Merkle proof
   
   Recomputes root hash from tx-hash and proof path.
   Returns true if computed root matches expected root."
  [tx-hash proof-path expected-root]
  (when (sha-256-hex? expected-root)
    (log/info "Verifying Merkle proof against root:" (subs expected-root 0 16) "..."))
  (if (or (not (sha-256-hex? tx-hash))
          (not (sha-256-hex? expected-root))
          (not (sequential? proof-path))
          (not-every? valid-proof-step? proof-path))
    false
    (let [computed-root (reduce (fn [current-hash {:keys [hash direction]}]
                                  (case (normalize-direction direction)
                                    :left (hash-pair hash current-hash)
                                    :right (hash-pair current-hash hash)))
                                tx-hash
                                proof-path)]
      (= computed-root expected-root))))

;; ============================================================================
;; Batch Verification
;; ============================================================================

(defn verify-batch
  "Verify multiple transactions in one operation"
  [merkle-root tx-hashes proof-paths]
  (and (sequential? tx-hashes)
       (sequential? proof-paths)
       (= (count tx-hashes) (count proof-paths))
       (every? true?
               (map #(verify-proof %1 %2 merkle-root)
                    tx-hashes
                    proof-paths))))

;; ============================================================================
;; Helper Functions
;; ============================================================================

(defn get-merkle-root
  "Get root hash of Merkle tree"
  [merkle-tree]
  (:root merkle-tree))

(defn get-proof-size
  "Calculate size of Merkle proof in bytes"
  [proof]
  (* 32 (count (:proof-path proof)))) ;; 32 bytes per SHA-256 hash

;; Logging stub
(defn- log-info [& args])
