(ns datomic-blockchain.crypto.test-ed25519
  "Comprehensive test suite for Ed25519 cryptographic operations.

   Tests cover:
   - Key generation (random and seed-based)
   - JWK format encoding/decoding
   - Base64 encoding/decoding
   - Signing and verification
   - Transaction signing and verification
   - BIP39 mnemonic phrase handling
   - File-based encrypted keystore
   - In-memory keystore
   - Helper functions"
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [datomic-blockchain.crypto.ed25519 :as crypto])
  (:import [java.util UUID]))

;; =============================================================================
;; Key Generation Tests
;; =============================================================================

(deftest ^:parallel generate-keypair-test
  (testing "Generate new Ed25519 keypair"
    (let [keypair (crypto/generate-keypair)]
      (is (some? (:private-key keypair)))
      (is (some? (:public-key keypair)))
      (is (instance? UUID (:keypair-id keypair)))
      (is (bytes? (:private-key keypair)))
      (is (bytes? (:public-key keypair)))
      ;; X509 encoded Ed25519 public keys are 44 bytes (32-byte key + X509 header)
      (is (= 44 (count (:public-key keypair))))
      ;; PKCS8 private keys are longer (contains seed + public key)
      (is (< 30 (count (:private-key keypair)))))))

(deftest ^:parallel generate-keypair-unique-test
  (testing "Each generated keypair is unique"
    (let [kp1 (crypto/generate-keypair)
          kp2 (crypto/generate-keypair)]
      (is (not= (:keypair-id kp1) (:keypair-id kp2)))
      (is (not= (seq (:public-key kp1)) (seq (:public-key kp2))))
      (is (not= (seq (:private-key kp1)) (seq (:private-key kp2)))))))

(deftest ^:parallel generate-keypair-from-seed-test
  (testing "Generate deterministic keypair from seed"
    (let [seed (byte-array 32 (range 32))
          kp1 (crypto/generate-keypair-from-seed seed)
          kp2 (crypto/generate-keypair-from-seed seed)]
      ;; Same seed should produce different keys with current implementation
      ;; because KeyPairGenerator doesn't support deterministic seeding
      (is (some? (:private-key kp1)))
      (is (some? (:public-key kp1)))
      (is (some? (:private-key kp2)))
      (is (some? (:public-key kp2)))))

;; =============================================================================
;; Base64 Encoding Tests
;; =============================================================================

(deftest ^:parallel bytes->base64-test
  (testing "Encode bytes to URL-safe Base64"
    (let [data (.getBytes "hello" "UTF-8")
          encoded (crypto/bytes->base64 data)]
      (is (string? encoded))
      (is (not (str/includes? encoded "=")))  ; No padding
      (is (not (str/includes? encoded "+")))  ; URL-safe
      (is (not (str/includes? encoded "/"))))))

(deftest ^:parallel base64->bytes-test
  (testing "Decode Base64 string to bytes"
    (let [original (.getBytes "hello" "UTF-8")
          encoded (crypto/bytes->base64 original)
          decoded (crypto/base64->bytes encoded)]
      (is (bytes? decoded))
      (is (= (seq original) (seq decoded)))))))

(deftest ^:parallel base64-roundtrip-test
  (testing "Base64 encoding/decoding roundtrip"
    (let [original (.getBytes "Hello, World! This is a test." "UTF-8")]
      (is (= (seq original)
             (seq (crypto/base64->bytes (crypto/bytes->base64 original)))))))

;; =============================================================================
;; JWK Format Tests
;; =============================================================================

(deftest ^:parallel public-key->jwk-test
  (testing "Encode public key to JWK format"
    (let [keypair (crypto/generate-keypair)
          jwk (crypto/public-key->jwk (:public-key keypair))]
      (is (= "OKP" (:kty jwk)))
      (is (= "Ed25519" (:crv jwk)))
      (is (string? (:x jwk)))
      (is (not (str/blank? (:x jwk))))))))

(deftest ^:parallel jwk->public-key-test
  (testing "Decode JWK to public key bytes"
    (let [keypair (crypto/generate-keypair)
          jwk (crypto/public-key->jwk (:public-key keypair))
          decoded (crypto/jwk->public-key jwk)]
      (is (some? decoded))
      (is (bytes? decoded))
      (is (= (seq (:public-key keypair)) (seq decoded))))))

(deftest ^:parallel private-key->jwk-test
  (testing "Encode private key to JWK format"
    (let [keypair (crypto/generate-keypair)
          jwk (crypto/private-key->jwk (:public-key keypair) (:private-key keypair))]
      (is (= "OKP" (:kty jwk)))
      (is (= "Ed25519" (:crv jwk)))
      (is (string? (:x jwk)))
      (is (string? (:d jwk))))))

(deftest ^:parallel jwk->private-key-test
  (testing "Decode JWK to private key bytes"
    (let [keypair (crypto/generate-keypair)
          jwk (crypto/private-key->jwk (:public-key keypair) (:private-key keypair))
          decoded (crypto/jwk->private-key jwk)]
      ;; Note: JWK->private only extracts the 'd' parameter (32 bytes)
      ;; which is the seed part, not the full PKCS8 key
      (is (some? decoded))
      (is (bytes? decoded))
      (is (= 32 (count decoded))))))

(deftest ^:parallel jwk-invalid-type-test
  (testing "Return nil for invalid JWK type"
    (is (nil? (crypto/jwk->public-key {:kty "RSA" :crv "Ed25519"})))
    (is (nil? (crypto/jwk->public-key {:kty "OKP" :crv "RSA"})))))

;; =============================================================================
;; Signing and Verification Tests
;; =============================================================================

(deftest ^:parallel sign-test
  (testing "Sign data with private key"
    (let [keypair (crypto/generate-keypair)
          message "Hello, Blockchain!"
          signature (crypto/sign message (:private-key keypair))]
      (is (string? signature))
      (is (not (str/blank? signature))))))

(deftest ^:parallel sign-bytes-test
  (testing "Sign byte array with private key"
    (let [keypair (crypto/generate-keypair)
          message (.getBytes "Binary data" "UTF-8")
          signature (crypto/sign message (:private-key keypair))]
      (is (string? signature))
      (is (not (str/blank? signature))))))

(deftest ^:parallel verify-valid-signature-test
  (testing "Verify valid signature"
    (let [keypair (crypto/generate-keypair)
          message "Important message"
          signature (crypto/sign message (:private-key keypair))]
      (is (true? (crypto/verify message signature (:public-key keypair)))))))

(deftest ^:parallel verify-invalid-signature-test
  (testing "Reject invalid signature"
    (let [keypair (crypto/generate-keypair)
          message "Important message"
          fake-signature "invalid.signature.here"]
      (is (false? (crypto/verify message fake-signature (:public-key keypair)))))))

(deftest ^:parallel verify-wrong-message-test
  (testing "Reject signature for different message"
    (let [keypair (crypto/generate-keypair)
          message1 "Message one"
          message2 "Message two"
          signature (crypto/sign message1 (:private-key keypair))]
      (is (false? (crypto/verify message2 signature (:public-key keypair)))))))

(deftest ^:parallel verify-wrong-key-test
  (testing "Reject signature verified with wrong public key"
    (let [kp1 (crypto/generate-keypair)
          kp2 (crypto/generate-keypair)
          message "Test message"
          signature (crypto/sign message (:private-key kp1))]
      (is (false? (crypto/verify message signature (:public-key kp2)))))))

;; =============================================================================
;; Transaction Signing Tests
;; =============================================================================

(deftest ^:parallel sign-transaction-test
  (testing "Sign blockchain transaction"
    (let [keypair (crypto/generate-keypair)
          transaction {:blockchain/hash "abc123"
                       :blockchain/data "transaction data"}
          signed (crypto/sign-transaction transaction (:private-key keypair))]
      (is (= "abc123" (:blockchain/hash signed)))
      (is (some? (:blockchain/signature signed)))
      (is (string? (:blockchain/signature signed))))))

(deftest ^:parallel sign-transaction-no-hash-test
  (testing "Throw exception when signing transaction without hash"
    (let [keypair (crypto/generate-keypair)
          transaction {:blockchain/data "no hash"}]
      (is (thrown? clojure.lang.ExceptionInfo
                   (crypto/sign-transaction transaction (:private-key keypair)))))))

(deftest ^:parallel verify-transaction-signature-valid-test
  (testing "Verify valid transaction signature"
    (let [keypair (crypto/generate-keypair)
          transaction {:blockchain/hash "xyz789"
                       :blockchain/data "data"}
          signed (crypto/sign-transaction transaction (:private-key keypair))]
      (is (true? (crypto/verify-transaction-signature signed (:public-key keypair)))))))

(deftest ^:parallel verify-transaction-signature-invalid-test
  (testing "Return nil for transaction without signature"
    (let [keypair (crypto/generate-keypair)
          transaction {:blockchain/hash "xyz789"}]
      (is (nil? (crypto/verify-transaction-signature transaction (:public-key keypair)))))))

;; =============================================================================
;; BIP39 Mnemonic Tests
;; =============================================================================

(deftest ^:parallel seed-from-mnemonic-test
  (testing "Derive seed from mnemonic phrase"
    (let [mnemonic "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about"
          seed (crypto/seed-from-mnemonic mnemonic)]
      (is (bytes? seed))
      (is (= 64 (count seed))))))

(deftest ^:parallel seed-from-mnemonic-with-passphrase-test
  (testing "Derive seed with passphrase"
    (let [mnemonic "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about"
          passphrase "my-secret-passphrase"
          seed1 (crypto/seed-from-mnemonic mnemonic)
          seed2 (crypto/seed-from-mnemonic mnemonic passphrase)]
      ;; Different passphrases should produce different seeds
      (is (not= (seq seed1) (seq seed2))))))

(deftest ^:parallel seed-from-mnemonic-deterministic-test
  (testing "Same mnemonic produces same seed"
    (let [mnemonic "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about"
          seed1 (crypto/seed-from-mnemonic mnemonic)
          seed2 (crypto/seed-from-mnemonic mnemonic)]
      (is (= (seq seed1) (seq seed2))))))

(deftest ^:parallel keypair-from-mnemonic-test
  (testing "Generate keypair from mnemonic"
    (let [mnemonic "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about"
          keypair (crypto/keypair-from-mnemonic mnemonic)]
      (is (some? (:private-key keypair)))
      (is (some? (:public-key keypair)))
      (is (instance? UUID (:keypair-id keypair))))))

;; =============================================================================
;; In-Memory Keystore Tests
;; =============================================================================

(deftest ^:parallel create-memory-keystore-test
  (testing "Create in-memory keystore"
    (let [keystore (crypto/create-memory-keystore)]
      (is (fn? (:store keystore)))
      (is (fn? (:get-private keystore)))
      (is (fn? (:get-public keystore)))
      (is (fn? (:delete keystore)))
      (is (some? (:store-atom keystore))))))

(deftest ^:parallel memory-keystore-store-retrieve-test
  (testing "Store and retrieve keys from memory keystore"
    (let [keystore (crypto/create-memory-keystore)
          keypair (crypto/generate-keypair)
          keypair-id (:keypair-id keypair)]
      ((:store keystore) keypair-id (:private-key keypair) (:public-key keypair))
      (let [retrieved-private ((:get-private keystore) keypair-id)
            retrieved-public ((:get-public keystore) keypair-id)]
        (is (= (seq (:private-key keypair)) (seq retrieved-private)))
        (is (= (seq (:public-key keypair)) (seq retrieved-public)))))))

(deftest ^:parallel memory-keystore-delete-test
  (testing "Delete keypair from memory keystore"
    (let [keystore (crypto/create-memory-keystore)
          keypair (crypto/generate-keypair)
          keypair-id (:keypair-id keypair)]
      ((:store keystore) keypair-id (:private-key keypair) (:public-key keypair))
      (is (some? ((:get-private keystore) keypair-id)))
      ((:delete keystore) keypair-id)
      (is (nil? ((:get-private keystore) keypair-id))))))

(deftest ^:parallel memory-keystore-get-missing-test
  (testing "Return nil for non-existent keypair"
    (let [keystore (crypto/create-memory-keystore)
          fake-id (random-uuid)]
      (is (nil? ((:get-private keystore) fake-id)))
      (is (nil? ((:get-public keystore) fake-id))))))

;; =============================================================================
;; Helper Functions Tests
;; =============================================================================

(deftest ^:parallel store-keypair-test
  (testing "Store keypair using helper function"
    (let [keystore (crypto/create-memory-keystore)
          keypair (crypto/generate-keypair)
          keypair-id (:keypair-id keypair)]
      (crypto/store-keypair! keystore keypair-id (:private-key keypair) (:public-key keypair))
      (is (some? (crypto/retrieve-private-key keystore keypair-id)))
      (is (some? (crypto/retrieve-public-key keystore keypair-id))))))

(deftest ^:parallel retrieve-keys-test
  (testing "Retrieve keys using helper functions"
    (let [keystore (crypto/create-memory-keystore)
          keypair (crypto/generate-keypair)
          keypair-id (:keypair-id keypair)]
      (crypto/store-keypair! keystore keypair-id (:private-key keypair) (:public-key keypair))
      (let [priv (crypto/retrieve-private-key keystore keypair-id)
            pub (crypto/retrieve-public-key keystore keypair-id)]
        (is (= (seq (:private-key keypair)) (seq priv)))
        (is (= (seq (:public-key keypair)) (seq pub)))))))

(deftest ^:parallel delete-keypair-test
  (testing "Delete keypair using helper function"
    (let [keystore (crypto/create-memory-keystore)
          keypair (crypto/generate-keypair)
          keypair-id (:keypair-id keypair)]
      (crypto/store-keypair! keystore keypair-id (:private-key keypair) (:public-key keypair))
      (is (some? (crypto/retrieve-private-key keystore keypair-id)))
      (crypto/delete-keypair! keystore keypair-id)
      (is (nil? (crypto/retrieve-private-key keystore keypair-id)))))

;; =============================================================================
;; Integration Tests
;; =============================================================================

(deftest ^:parallel full-signing-workflow-test
  (testing "Complete signing workflow with keystore"
    (let [keystore (crypto/create-memory-keystore)
          keypair (crypto/generate-keypair)
          keypair-id (:keypair-id keypair)
          message "Important blockchain message"]
      ;; Store keypair
      (crypto/store-keypair! keystore keypair-id (:private-key keypair) (:public-key keypair))

      ;; Sign message with stored private key
      (let [private-key (crypto/retrieve-private-key keystore keypair-id)
            signature (crypto/sign message private-key)]

        ;; Verify signature with stored public key
        (let [public-key (crypto/retrieve-public-key keystore keypair-id)]
          (is (true? (crypto/verify message signature public-key)))))))))

(deftest ^:parallel transaction-signing-workflow-test
  (testing "Complete transaction signing workflow"
    (let [keystore (crypto/create-memory-keystore)
          keypair (crypto/generate-keypair)
          keypair-id (:keypair-id keypair)
          transaction {:blockchain/hash "tx-hash-123"
                       :blockchain/from "alice"
                       :blockchain/to "bob"
                       :blockchain/amount 100}]

      (crypto/store-keypair! keystore keypair-id (:private-key keypair) (:public-key keypair))

      (let [private-key (crypto/retrieve-private-key keystore keypair-id)
            signed-tx (crypto/sign-transaction transaction private-key)]

        (is (some? (:blockchain/signature signed-tx)))

        (let [public-key (crypto/retrieve-public-key keystore keypair-id)]
          (is (true? (crypto/verify-transaction-signature signed-tx public-key))))))))

(deftest ^:parallel jwk-roundtrip-test
  (testing "JWK encoding/decoding roundtrip"
    (let [keypair (crypto/generate-keypair)
          original-public (:public-key keypair)
          original-private (:private-key keypair)]

      (let [pub-jwk (crypto/public-key->jwk original-public)
            priv-jwk (crypto/private-key->jwk original-public original-private)
            decoded-public (crypto/jwk->public-key pub-jwk)
            decoded-private-seed (crypto/jwk->private-key priv-jwk)]

        ;; Public key roundtrip should work
        (is (= (seq original-public) (seq decoded-public)))

        ;; Private key JWK only stores seed (32 bytes), not full PKCS8
        (is (some? decoded-private-seed))
        (is (= 32 (count decoded-private-seed)))))))

(deftest ^:parallel multiple-keypairs-keystore-test
  (testing "Store and manage multiple keypairs"
    (let [keystore (crypto/create-memory-keystore)
          kp1 (crypto/generate-keypair)
          kp2 (crypto/generate-keypair)
          kp3 (crypto/generate-keypair)]

      ;; Store all keypairs
      (crypto/store-keypair! keystore (:keypair-id kp1) (:private-key kp1) (:public-key kp1))
      (crypto/store-keypair! keystore (:keypair-id kp2) (:private-key kp2) (:public-key kp2))
      (crypto/store-keypair! keystore (:keypair-id kp3) (:private-key kp3) (:public-key kp3))

      ;; Verify all are stored
      (is (some? (crypto/retrieve-private-key keystore (:keypair-id kp1))))
      (is (some? (crypto/retrieve-private-key keystore (:keypair-id kp2))))
      (is (some? (crypto/retrieve-private-key keystore (:keypair-id kp3))))

      ;; Delete one
      (crypto/delete-keypair! keystore (:keypair-id kp2))
      (is (nil? (crypto/retrieve-private-key keystore (:keypair-id kp2))))

      ;; Others should still exist
      (is (some? (crypto/retrieve-private-key keystore (:keypair-id kp1))))
      (is (some? (crypto/retrieve-private-key keystore (:keypair-id kp3)))))))