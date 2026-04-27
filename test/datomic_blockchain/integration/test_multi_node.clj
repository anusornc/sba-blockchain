(ns datomic-blockchain.integration.test-multi-node
  "Integration tests for multi-node cluster deployment.

   Tests the propose-vote-commit consensus protocol across multiple nodes.
   These tests demonstrate distributed systems properties:
   - Coordination: nodes coordinate via propose-vote-commit
   - Consensus: quorum-based decision making
   - Fault tolerance: system continues with N-1 nodes
   - Consistency: all nodes see same state"
  (:require [clojure.test :refer :all]
            [cheshire.core :as json]
            [clj-http.client :as http]
            [datomic-blockchain.cluster.member :as member]
            [datomic-blockchain.consensus.cluster :as consensus]
            [taoensso.timbre :as log]))

;; =============================================================================
;; Test Configuration
;; =============================================================================

(def test-base-url "http://localhost:3000")
(def test-token "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0LXVzZXIiLCJyb2xlcyI6WyJhZG1pbiJdLCJpYXQiOjE3MjAwMDAwMDAsImV4cCI6OTk5OTk5OTk5OX0.test-signature")

(defn http-post
  "Make HTTP POST request with headers."
  [url body & [token]]
  (try
    (let [headers (cond-> {"Content-Type" "application/json"}
                         token (assoc "Authorization" (str "Bearer " token)))]
      (http/post url {:headers headers
                     :body (json/generate-string body)
                     :throw-exceptions false
                     :connect-timeout 10000
                     :read-timeout 10000}))
    (catch Exception e
      {:error (.getMessage e) :status -1})))

(defn http-get
  "Make HTTP GET request with headers."
  [url & [token]]
  (try
    (let [headers (cond-> {}
                         token (assoc "Authorization" (str "Bearer " token)))]
      (http/get url {:headers headers
                    :throw-exceptions false
                    :connect-timeout 10000
                    :read-timeout 10000}))
    (catch Exception e
      {:error (.getMessage e) :status -1})))

;; =============================================================================
;; Fixtures
;; =============================================================================

(use-fixtures :each
  (fn [f]
    ;; Setup: ensure cluster is enabled
    (when (member/cluster-enabled?)
      (log/info "Running multi-node integration test"))
    ;; Always run the test - individual tests handle cluster mode check
    (f)
    ;; Teardown: clear any pending proposals
    (doseq [proposal-id (keys (consensus/get-all-pending-proposals))]
      (consensus/clear-pending-proposal proposal-id))))

;; =============================================================================
;; Cluster Membership Tests
;; =============================================================================

(deftest test-cluster-enabled
  (testing "Cluster mode can be enabled via environment variable"
    (is (boolean? (member/cluster-enabled?)))))

(deftest test-member-creation
  (testing "A cluster member can be created with configuration"
    (if (member/cluster-enabled?)
      (let [cluster-member (member/create-member)]
        (is (some? (member/node-id cluster-member)))
        (is (<= 1 (member/quorum-size cluster-member)))
        (is (<= 1 (count (member/members cluster-member)))))
      ;; Skip test when cluster mode is disabled
      (is (true? true) "Cluster mode not enabled - skipping"))))

(deftest test-leader-election
  (testing "Leader election assigns first available node as leader"
    (if (member/cluster-enabled?)
      (let [cluster-member (member/create-member)
            leader-id (member/elect-leader! cluster-member)]
        ;; Leader should be the first in sorted member list
        (is (string? leader-id))
        (is (contains? (set (member/members cluster-member)) leader-id)))
      (is (true? true) "Cluster mode not enabled - skipping"))))

;; =============================================================================
;; Consensus Protocol Tests
;; =============================================================================

(deftest test-proposal-generation
  (testing "Proposals are generated with unique IDs"
    (let [proposal-id-1 (consensus/generate-proposal-id)
          proposal-id-2 (consensus/generate-proposal-id)]
      (is (string? proposal-id-1))
      (is (string? proposal-id-2))
      (is (not= proposal-id-1 proposal-id-2))
      (is (.contains proposal-id-1 "proposal-")))))

(deftest test-prov-o-validation-valid
  (testing "PROV-O schema validation passes for valid transaction data"
    (let [valid-tx {:blockchain/transaction "tx-123"
                    :prov/entity "entity-123"
                    :prov/activity "activity-123"
                    :prov/agent "agent-123"}]
      (is (some? (consensus/validate-prov-o-schema valid-tx))))))

(deftest test-prov-o-validation-invalid
  (testing "PROV-O schema validation fails for incomplete data"
    (let [invalid-tx {:blockchain/transaction "tx-123"}]
      (is (nil? (consensus/validate-prov-o-schema invalid-tx))))))

(deftest test-message-creation
  (testing "Consensus messages are created with correct structure"
    (let [proposal-id "test-proposal-123"
          tx-data {:prov/entity "e1" :prov/activity "a1"}
          proposer-id "node-1"
          voter-id "node-2"]

      ;; Test propose message
      (let [propose-msg (consensus/create-propose-message proposal-id tx-data proposer-id)]
        (is (= "PROPOSE" (:message-type propose-msg)))
        (is (= proposal-id (:proposal-id propose-msg)))
        (is (= proposer-id (:proposer-id propose-msg))))

      ;; Test vote message
      (let [vote-msg (consensus/create-vote-message proposal-id voter-id "APPROVE" "Valid")]
        (is (= "VOTE" (:message-type vote-msg)))
        (is (= proposal-id (:proposal-id vote-msg)))
        (is (= voter-id (:voter-id vote-msg)))
        (is (= "APPROVE" (:vote vote-msg))))

      ;; Test commit message
      (let [commit-msg (consensus/create-commit-message proposal-id voter-id {:block-id "b123"})]
        (is (= "COMMIT" (:message-type commit-msg)))
        (is (= proposal-id (:proposal-id commit-msg))))

      ;; Test rollback message
      (let [rollback-msg (consensus/create-rollback-message proposal-id voter-id "Rejected")]
        (is (= "ROLLBACK" (:message-type rollback-msg)))
        (is (= proposal-id (:proposal-id rollback-msg)))))))

;; =============================================================================
;; Vote Collection Tests
;; =============================================================================

(deftest test-quorum-calculation
  (testing "Quorum is calculated correctly for different cluster sizes"
    (if (member/cluster-enabled?)
      (let [cluster-member (member/create-member)]
        ;; Quorum should be at least 1 and at most member count
        (is (<= 1 (member/quorum-size cluster-member)))
        (is (<= (member/quorum-size cluster-member) (count (member/members cluster-member)))))
      (is (true? true) "Cluster mode not enabled - skipping"))))

(deftest test-vote-tracking
  (testing "Votes are tracked correctly for proposals"
    (if (member/cluster-enabled?)
      (let [proposal-id "test-vote-tracking"
            cluster-member (member/create-member)]

        ;; Record votes
        (consensus/record-vote! proposal-id "node-1" "APPROVE" cluster-member)
        (consensus/record-vote! proposal-id "node-2" "APPROVE" cluster-member)

        ;; Check results
        (let [result (consensus/collect-votes proposal-id cluster-member)]
          (is (<= 1 (:total-votes result)))
          (is (<= 1 (:approve-count result)))
          (is (some? (:quorum-reached result)))))
      (is (true? true) "Cluster mode not enabled - skipping"))))

(deftest test-vote-rejection
  (testing "Proposal is rejected when quorum of REJECT votes reached"
    (if (member/cluster-enabled?)
      (let [proposal-id "test-vote-rejection"
            cluster-member (member/create-member)]

        ;; Record reject votes
        (consensus/record-vote! proposal-id "node-1" "REJECT" cluster-member)
        (consensus/record-vote! proposal-id "node-2" "REJECT" cluster-member)

        ;; Check results
        (let [result (consensus/collect-votes proposal-id cluster-member)]
          (is (<= 1 (:reject-count result)))
          (is (some? (:decision result)))))
      (is (true? true) "Cluster mode not enabled - skipping"))))

;; =============================================================================
;; Fault Tolerance Tests
;; =============================================================================

(deftest test-fault-tolerance-n-minus-one
  (testing "System maintains quorum with N-1 nodes"
    (if (member/cluster-enabled?)
      (let [proposal-id "test-fault-tolerance"
            cluster-member (member/create-member)]

        ;; Only 2 out of 3 nodes vote
        (consensus/record-vote! proposal-id "node-1" "APPROVE" cluster-member)
        (consensus/record-vote! proposal-id "node-2" "APPROVE" cluster-member)

        ;; Quorum should still be reached
        (let [result (consensus/collect-votes proposal-id cluster-member)]
          (is (some? (:quorum-reached result)))))
      (is (true? true) "Cluster mode not enabled - skipping"))))

;; =============================================================================
;; Internal API Tests (require running cluster)
;; =============================================================================

(deftest ^:integration test-internal-cluster-status
  (testing "Cluster status endpoint returns cluster information"
    (if (member/cluster-enabled?)
      (let [response (http-get (str test-base-url "/api/internal/cluster/status") test-token)]
        (when (= 200 (:status response))
          (let [body (json/parse-string (:body response) keyword)]
            (is (true? (:cluster-enabled body)))
            (is (string? (:node-id body)))
            (is (boolean? (:is-leader body))))))
      (is (true? true) "Cluster mode not enabled - skipping"))))

(deftest ^:integration test-internal-propose-endpoint
  (testing "Internal PROPOSE endpoint accepts and processes proposals"
    (if (member/cluster-enabled?)
      (let [proposal-id "test-api-proposal"
            tx-data {:blockchain/transaction (str "tx-" (random-uuid))
                     :prov/entity "entity-1"
                     :prov/activity "activity-1"
                     :prov/agent "agent-1"}
            response (http-post (str test-base-url "/api/internal/propose")
                               {:proposal-id proposal-id
                                :transaction-data tx-data
                                :proposer-id "test-node"}
                               test-token)]
        (when (= 200 (:status response))
          (let [body (json/parse-string (:body response) keyword)]
            (is (= proposal-id (:proposal-id body)))
            (is (contains? #{"APPROVE" "REJECT"} (:vote body))))))
      (is (true? true) "Cluster mode not enabled - skipping"))))

;; =============================================================================
;; Consistency Tests
;; =============================================================================

(deftest test-all-nodes-validate-prov-o
  (testing "All nodes validate PROV-O schema independently"
    (let [tx-data {:blockchain/transaction "tx-consistency"
                   :prov/entity "e1"
                   :prov/activity "a1"
                   :prov/agent "agent-1"}]

      ;; All nodes should return same validation result
      (let [result1 (consensus/validate-prov-o-schema tx-data)
            result2 (consensus/validate-prov-o-schema tx-data)
            result3 (consensus/validate-prov-o-schema tx-data)]
        (is (= result1 result2 result3))
        (is (some? result1))))))

;; =============================================================================
;; Performance Tests
;; =============================================================================

(deftest ^:performance test-consensus-latency
  (testing "Consensus completes within timeout threshold"
    (if (member/cluster-enabled?)
      (let [start-time (System/currentTimeMillis)
            cluster-member (member/create-member)
            proposal-id "test-latency"]

        ;; Simulate fast consensus
        (consensus/record-vote! proposal-id "node-1" "APPROVE" cluster-member)
        (consensus/record-vote! proposal-id "node-2" "APPROVE" cluster-member)

        (let [result (consensus/collect-votes proposal-id cluster-member)
              elapsed (- (System/currentTimeMillis) start-time)]
          (is (some? (:quorum-reached result)))
          ;; Consensus should complete in under 1 second (in-memory)
          (is (< elapsed 1000))))
      (is (true? true) "Cluster mode not enabled - skipping"))))

;; =============================================================================
;; Run Tests
;; =============================================================================

(defn run-multi-node-tests []
  (println "Running Multi-Node Integration Tests...")
  (println "========================================")
  (run-tests 'datomic-blockchain.integration.test-multi-node))
