(ns datomic-blockchain.cluster.test-member
  "Comprehensive test suite for cluster member management.

   Tests cover:
   - Member creation and initialization
   - Leader election logic
   - Cluster member parsing from environment variables
   - Quorum size calculation
   - Cluster state management"
  (:require [clojure.test :refer :all]
            [datomic-blockchain.cluster.member :as member]))

;; =============================================================================
;; Helper Functions
;; ============================================================================

(defn- create-test-member
  "Create a test Member record with minimal dependencies."
  ([node-id members]
   (create-test-member node-id members 2))
  ([node-id members quorum]
   (member/->Member node-id
                     3000
                     members
                     quorum
                     (atom nil)
                     5000)))

;; =============================================================================
;; Fixtures
;; ============================================================================

(defn reset-cluster-state-fixture
  "Reset cluster state before each test."
  [f]
  (reset! member/cluster-state nil)
  (f))

(use-fixtures :each reset-cluster-state-fixture)

;; =============================================================================
;; Cluster Member Parsing Tests
;; ============================================================================

(deftest parse-cluster-members-simple-port-format-test
  (testing "Parse cluster members with simple node-id:port format"
    (let [env-value "node-1:3001,node-2:3002,node-3:3003"
          result (member/parse-cluster-members env-value)]
      (is (= 3 (count result)))
      (is (= "http://localhost:3001" (get result "node-1")))
      (is (= "http://localhost:3002" (get result "node-2")))
      (is (= "http://localhost:3003" (get result "node-3"))))))

(deftest parse-cluster-members-host-port-format-test
  (testing "Parse cluster members with node-id:host:port format"
    (let [env-value "node-1:172.28.0.11:3001,node-2:172.28.0.12:3002"
          result (member/parse-cluster-members env-value)]
      (is (= 2 (count result)))
      (is (= "http://172.28.0.11:3001" (get result "node-1")))
      (is (= "http://172.28.0.12:3002" (get result "node-2")))))

(deftest parse-cluster-members-mixed-format-test
  (testing "Parse cluster members with mixed formats"
    (let [env-value "node-1:3001,node-2:172.28.0.12:3002"
          result (member/parse-cluster-members env-value)]
      (is (= 2 (count result)))
      (is (= "http://localhost:3001" (get result "node-1")))
      (is (= "http://172.28.0.12:3002" (get result "node-2")))))

(deftest parse-cluster-members-empty-string-test
  (testing "Return nil for empty environment variable"
    (is (nil? (member/parse-cluster-members "")))
    (is (nil? (member/parse-cluster-members nil)))))

(deftest parse-cluster-members-ignores-invalid-entries-test
  (testing "Ignore entries with insufficient parts"
    (let [env-value "node-1:3001,invalid-entry,node-2:3002"
          result (member/parse-cluster-members env-value)]
      (is (= 2 (count result)))
      (is (contains? result "node-1"))
      (is (contains? result "node-2"))
      (is (not (contains? result "invalid-entry"))))))

(deftest parse-cluster-members-with-spaces-test
  (testing "Handle cluster members string with extra spaces"
    (let [env-value " node-1 : 3001 , node-2 : 3002 "
          result (member/parse-cluster-members env-value)]
      (is (= 2 (count result)))))

(deftest parse-cluster-members-single-entry-test
  (testing "Handle single cluster member"
    (let [env-value "node-1:3001"
          result (member/parse-cluster-members env-value)]
      (is (= 1 (count result)))
      (is (= "http://localhost:3001" (get result "node-1"))))))

;; =============================================================================
;; Member Record Tests
;; ============================================================================

(deftest member-record-protocol-implementation-test
  (testing "Member implements ClusterMember protocol"
    (let [members {"node-1" "http://localhost:3001"
                  "node-2" "http://localhost:3002"}
          m (create-test-member "node-1" members)]
      (is (= "node-1" (member/node-id m)))
      (is (= members (member/members m)))
      (is (= 2 (member/quorum-size m))))))

;; =============================================================================
;; Leader Election Tests
;; ============================================================================

(deftest elect-leader-selects-first-sorted-test
  (testing "Leader election selects first node in sorted order"
    (let [members {"node-3" "http://localhost:3003"
                  "node-1" "http://localhost:3001"
                  "node-2" "http://localhost:3002"}
          m (create-test-member "node-1" members)]
      (member/elect-leader! m)
      (is (= "node-1" (member/leader-id m))))))

(deftest elect-leader-updates-leader-atom-test
  (testing "Leader election updates the leader atom"
    (let [members {"node-1" "http://localhost:3001"
                  "node-2" "http://localhost:3002"}
          m (create-test-member "node-1" members)]
      (is (nil? (member/leader-id m)))
      (member/elect-leader! m)
      (is (some? (member/leader-id m))))))

(deftest is-leader-returns-correct-value-test
  (testing "is-leader? returns true only for leader node"
    (let [members {"node-1" "http://localhost:3001"
                  "node-2" "http://localhost:3002"}
          m1 (create-test-member "node-1" members)
          m2 (create-test-member "node-2" members)]
      (member/elect-leader! m1)
      (is (true? (member/is-leader? m1)))
      (is (false? (member/is-leader? m2))))))

;; =============================================================================
;; Quorum Size Tests
;; ============================================================================

(deftest quorum-size-two-node-cluster-test
  (testing "Quorum of 2 requires both nodes"
    (let [m (create-test-member "node-1" {"node-1" "n1" "node-2" "n2"} 2)]
      (is (= 2 (member/quorum-size m))))))

)

(deftest quorum-size-three-node-cluster-test
  (testing "Quorum of 3 requires 2 nodes (majority)"
    (let [m (create-test-member "node-1" {"node-1" "n1" "node-2" "n2" "node-3" "n3"} 2)]
      (is (= 2 (member/quorum-size m))))))

)

(deftest quorum-size-five-node-cluster-test
  (testing "Quorum of 5 requires 3 nodes (majority)"
    (let [m (create-test-member "node-1"
                               (into {} (for [i (range 1 6)]
                                             [(str "node-" i) (str "n" i)]))
                               3)]
      (is (= 3 (member/quorum-size m))))))

)

;; =============================================================================
;; Cluster State Tests
;; ============================================================================

(deftest get-cluster-returns-stored-member-test
  (testing "get-cluster returns the stored cluster member"
    (let [test-member (create-test-member "test-node" {"node-1" "n1"})]
      (reset! member/cluster-state test-member)
      (is (identical? test-member (member/get-cluster))))))

(deftest get-cluster-returns-nil-when-uninitialized-test
  (testing "get-cluster returns nil when cluster not initialized"
    (reset! member/cluster-state nil)
    (is (nil? (member/get-cluster)))))

;; =============================================================================
;; Send Message Tests
;; ============================================================================

(deftest send-message-to-unknown-member-test
  (testing "Send message to unknown member returns nil"
    (let [members {"node-1" "http://localhost:3001"}
          m (create-test-member "node-1" members)]
      (is (nil? (member/send-message m "unknown-node" "/test" "{}"))))))
