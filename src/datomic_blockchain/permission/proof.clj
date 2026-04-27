(ns datomic-blockchain.permission.proof
  "Permission proof generation and verification.

  Proofs provide audit-grade evidence of access decisions by hashing a
  canonical payload and optionally signing it with Ed25519."
  (:require [clojure.string :as str]
            [taoensso.timbre :as log]
            [datomic-blockchain.crypto.ed25519 :as ed25519])
  (:import [java.security MessageDigest]
           [java.math BigInteger]
           [java.util Base64 UUID Date]))

;; ============================================================================
;; Canonicalization and Hashing
;; ============================================================================

(def ^:private proof-version 1)

(defn- base64-decode
  [^String value]
  (.decode (Base64/getDecoder) value))

(defn- base64-encode
  [^bytes value]
  (.encodeToString (Base64/getEncoder) value))

;; Forward declarations for mutual recursion
(declare canonical-value)

(defn- canonical-pairs
  [m]
  (->> m
       (map (fn [[k v]]
              [(str k) (canonical-value v)]))
       (sort-by first)
       vec))

(defn- canonical-value
  [value]
  (cond
    (uuid? value) (str value)
    (inst? value) (str (.toInstant ^Date value))
    (keyword? value) (str value)
    (map? value) (canonical-pairs value)
    (coll? value) (mapv canonical-value value)
    :else value))

(defn- canonical-string
  [m]
  (pr-str (canonical-pairs m)))

(defn- sha-256-hash
  [value]
  (let [digest (MessageDigest/getInstance "SHA-256")
        bytes (.getBytes (str value) "UTF-8")
        hash-bytes (.digest digest bytes)]
    (format "%064x" (BigInteger. 1 hash-bytes))))

(defn- hash-payload
  [m]
  (sha-256-hash (canonical-string m)))

;; ============================================================================
;; Proof Payload
;; ============================================================================

(defn- policy-hash-input
  [policy]
  (when policy
    (select-keys policy [:permission-id
                         :owner-id
                         :resource-id
                         :resource-type
                         :visibility
                         :authorized-parties
                         :valid-from
                         :valid-until])))

(defn- request-hash-input
  [request]
  (select-keys request [:requestor-id
                        :resource-id
                        :requested-action
                        :request-time
                        :request-context]))

(defn- proof-payload
  [policy request result]
  (let [decision (if (:success? result) :allow :deny)]
    {:proof/version proof-version
     :proof/resource-id (:resource-id request)
     :proof/requestor-id (:requestor-id request)
     :proof/action (:requested-action request)
     :proof/decision decision
     :proof/reason (:reason result)
     :proof/policy-hash (when policy (hash-payload (policy-hash-input policy)))
     :proof/request-hash (hash-payload (request-hash-input request))
     :proof/timestamp (:request-time request)}))

;; ============================================================================
;; Signing and Verification
;; ============================================================================

(defn signing-keys-from-env
  "Load signing keys from environment variables.

  PERMISSION_PROOF_PRIVATE_KEY_B64 - required for signing
  PERMISSION_PROOF_PUBLIC_KEY_B64  - required for verification/signing metadata
  PERMISSION_PROOF_KEY_ID          - optional key identifier"
  []
  (let [private-b64 (System/getenv "PERMISSION_PROOF_PRIVATE_KEY_B64")
        public-b64 (System/getenv "PERMISSION_PROOF_PUBLIC_KEY_B64")
        key-id (System/getenv "PERMISSION_PROOF_KEY_ID")]
    (when (and (not (str/blank? private-b64))
               (not (str/blank? public-b64)))
      {:private-key (base64-decode private-b64)
       :public-key (base64-decode public-b64)
       :key-id key-id
       :public-key-b64 public-b64})))

(defn sign-proof
  "Sign proof payload hash and attach signature fields."
  [proof private-key public-key-b64 key-id]
  (let [signature (ed25519/sign (:proof/hash proof) private-key)]
    (cond-> (assoc proof :proof/signature signature
                         :proof/signed? true)
      key-id (assoc :proof/signing-key-id key-id)
      public-key-b64 (assoc :proof/public-key public-key-b64))))

(defn verify-proof
  "Verify proof hash and optional signature.

  Returns map with :valid?, :hash-valid?, :signature-valid?, :signed?."
  [proof & [public-key-bytes]]
  (let [payload (select-keys proof [:proof/version
                                    :proof/resource-id
                                    :proof/requestor-id
                                    :proof/action
                                    :proof/decision
                                    :proof/reason
                                    :proof/policy-hash
                                    :proof/request-hash
                                    :proof/timestamp])
        expected-hash (hash-payload payload)
        hash-valid? (= expected-hash (:proof/hash proof))
        signed? (boolean (:proof/signature proof))
        public-key (or public-key-bytes
                       (when-let [b64 (:proof/public-key proof)]
                         (base64-decode b64)))
        signature-valid? (if signed?
                           (and public-key
                                (ed25519/verify expected-hash
                                                (:proof/signature proof)
                                                public-key))
                           true)]
    {:valid? (and hash-valid? signature-valid?)
     :hash-valid? hash-valid?
     :signature-valid? signature-valid?
     :signed? signed?}))

;; ============================================================================
;; Proof Construction
;; ============================================================================

(defn build-proof
  "Build a permission proof for an access decision.

  Options:
  - :signing - map with :private-key, :public-key-b64, :key-id
  - :include-public-key? - include public key in proof (default true)"
  [policy request result opts]
  (let [payload (proof-payload policy request result)
        proof (assoc payload
                     :proof/id (UUID/randomUUID)
                     :proof/hash (hash-payload payload))
        signing (or (:signing opts) (signing-keys-from-env))
        include-public-key? (get opts :include-public-key? true)]
    (if (and signing (:private-key signing))
      (sign-proof proof
                  (:private-key signing)
                  (when include-public-key? (:public-key-b64 signing))
                  (:key-id signing))
      (assoc proof :proof/signed? false))))
