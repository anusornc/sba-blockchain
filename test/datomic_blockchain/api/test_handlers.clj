(ns datomic-blockchain.api.test-handlers
  "Comprehensive test suite for API handlers.

   Tests cover:
   - Input validation (UUID, integers, string sanitization)
   - Health check endpoint
   - Graph handlers (get entity, get graph, find path)
   - Traceability handlers (trace product, provenance, timeline)
   - Query handlers (whitelist validation, query execution)
   - Ontology handlers (list, get)
   - Permission handlers
   - Statistics handlers
   - Error handlers
   - Cluster consensus handlers (propose, vote, commit, rollback)
   - Transaction submission/polling API"
  (:require [clojure.test :refer :all]
            [clojure.data.json :as json]
            [datomic-blockchain.api.handlers.core :as handlers]
            [datomic-blockchain.api.middleware :as middleware]
            [datomic-blockchain.datomic.schema :as schema]
            [datomic-blockchain.permission.policy :as policy]
            [datomic-blockchain.permission.model :as model]
            [datomic.api :as d])
  (:import [java.util UUID Date]))

;; =============================================================================
;; Helper Functions
;; =============================================================================

(defn parse-response-body
  "Parse JSON response body to Clojure map"
  [response]
  (when-let [body (:body response)]
    (json/read-str body :key-fn keyword)))

;; =============================================================================
;; Test Fixtures
;; =============================================================================

(def ^:dynamic *conn* nil)

(defn- temp-conn-fixture
  "Create a temporary in-memory Datomic connection for testing"
  [f]
  (let [uri "datomic:mem://test-handlers"]
    (d/delete-database uri)
    (d/create-database uri)
    (let [conn (d/connect uri)]
      ;; Install full schema for testing
      @(d/transact conn schema/full-schema)
      (binding [*conn* conn]
        (binding [handlers/*connection* conn]
          (f)))
      (d/release conn)))

(defn setup-policy-store
  "Initialize the policy store before permission tests"
  [f]
  (policy/init-policy-store (model/visibility-strategy))
  (f))

(use-fixtures :each temp-conn-fixture setup-policy-store)

;; =============================================================================
;; Input Validation Tests
;; =============================================================================

(deftest valid-uuid?-test
  (testing "Valid UUID formats are accepted"
    (is (true? (handlers/valid-uuid? "550e8400-e29b-41d4-a716-446655440000")))
    (is (true? (handlers/valid-uuid? "00000000-0000-0000-0000-000000000000")))
    (is (true? (handlers/valid-uuid? "FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF")))))

(deftest valid-uuid?-rejects-invalid-test
  (testing "Invalid UUID formats are rejected"
    (is (false? (handlers/valid-uuid? "not-a-uuid")))
    (is (false? (handlers/valid-uuid? "550e8400-e29b-41d4-a716")))
    (is (false? (handlers/valid-uuid? "")))
    (is (false? (handlers/valid-uuid? nil)))
    (is (false? (handlers/valid-uuid? 123)))))

(deftest parse-uuid-safe-test
  (testing "Valid UUID strings are parsed to UUID objects"
    (let [uuid-str "550e8400-e29b-41d4-a716-446655440000"
          parsed (handlers/parse-uuid-safe uuid-str)]
      (is (instance? UUID parsed))
      (is (= uuid-str (str parsed)))))
  (testing "Invalid UUID strings return nil"
    (is (nil? (handlers/parse-uuid-safe "invalid")))
    (is (nil? (handlers/parse-uuid-safe nil)))))

(deftest validate-uuid-param-valid-test
  (testing "Valid UUID parameter returns UUID object"
    (let [uuid-str "550e8400-e29b-41d4-a716-446655440000"
          result (handlers/validate-uuid-param :id uuid-str)]
      (is (instance? UUID result))
      (is (= uuid-str (str result))))))

(deftest validate-uuid-param-invalid-format-test
  (testing "Invalid UUID format throws ex-info"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Invalid UUID format"
                          (handlers/validate-uuid-param :id "not-a-uuid")))))

(deftest validate-positive-int-valid-test
  (testing "Valid positive integers are parsed"
    (is (= 10 (handlers/validate-positive-int :page "10" 10)))
    (is (= 0 (handlers/validate-positive-int :page "0" 10)))
    (is (= 5 (handlers/validate-positive-int :page nil 5)))))

(deftest validate-positive-int-invalid-test
  (testing "Invalid integers throw ex-info"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Invalid integer"
                          (handlers/validate-positive-int :page "abc" 10)))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"must be non-negative"
                          (handlers/validate-positive-int :page "-1" 10)))))

(deftest sanitize-string-param-valid-test
  (testing "Valid strings are returned as-is"
    (is (= "hello" (handlers/sanitize-string-param :name "hello" 100)))
    (is (= "test123" (handlers/sanitize-string-param :name "test123" 100)))))

(deftest sanitize-string-param-max-length-test
  (testing "Strings exceeding max length throw ex-info"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Parameter too long"
                          (handlers/sanitize-string-param :name (apply str (repeat 101 "a")) 100)))))

(deftest sanitize-string-param-suspicious-patterns-test
  (testing "Suspicious patterns throw ex-info"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Suspicious input"
                          (handlers/sanitize-string-param :name "<script>alert('xss')</script>" 100)))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Suspicious input"
                          (handlers/sanitize-string-param :name "javascript:alert(1)" 100)))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Suspicious input"
                          (handlers/sanitize-string-param :name "test onerror=bad" 100)))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Suspicious input"
                          (handlers/sanitize-string-param :name "test onload=bad" 100)))))

(deftest sanitize-string-param-nil-test
  (testing "Nil values return nil"
    (is (nil? (handlers/sanitize-string-param :name nil 100)))))

;; =============================================================================
;; Health Check Tests
;; =============================================================================

(deftest handle-health-test
  (testing "Health check returns healthy status"
    (let [response (handlers/handle-health {})
          body (parse-response-body response)]
      (is (= 200 (:status response)))
      (is (= :healthy (:data body))))))

(deftest handle-load-sample-data-benchmark-anchors-test
  (testing "Sample-data seed endpoint returns benchmark anchors and relationship count"
    (let [response (handlers/handle-load-sample-data {})
          body (parse-response-body response)
          data (:data body)
          anchors (:benchmark-anchors data)
          counts (:counts data)]
      (is (= 200 (:status response)))
      (is (true? (:success body)))
      (is (= "resources/datasets/uht-supply-chain/data.edn" (:dataset-source data)))
      (is (map? anchors))
      (is (string? (:qr-code anchors)))
      (is (not (empty? (:qr-code anchors))))
      (is (string? (:batch-id anchors)))
      (is (not (empty? (:batch-id anchors))))
      (is (some? (handlers/parse-uuid-safe (:entity-id anchors))))
      (is (some? (handlers/parse-uuid-safe (:activity-id anchors))))
      (is (= 22 (:relationships counts)))
      (is (pos? (:agents counts)))
      (is (pos? (:products counts)))
      (is (pos? (:activities counts))))))

;; =============================================================================
;; Query Whitelist Tests (SECURITY CRITICAL)
;; =============================================================================

(deftest query-allowed?-allowed-queries-test
  (testing "Pre-approved query templates are allowed"
    ;; get-entity-by-id
    (is (true? (:allowed (handlers/query-allowed? '[:find ?e :where [?e :db/id ?id]]))))
    ;; get-prov-entities
    (is (true? (:allowed (handlers/query-allowed? '[:find ?e :where [?e :prov/entity ?entity-id]]))))
    ;; get-prov-activities
    (is (true? (:allowed (handlers/query-allowed? '[:find ?a :where [?a :prov/activity ?activity-id]]))))
    ;; get-prov-agents
    (is (true? (:allowed (handlers/query-allowed? '[:find ?ag :where [?ag :prov/agent ?agent-id]]))))
    ;; count-entities
    (is (true? (:allowed (handlers/query-allowed? '[:find (count ?e) :where [?e :prov/entity]]))))))

(deftest query-allowed?-rejected-queries-test
  (testing "Queries not in whitelist are rejected"
    (is (false? (:allowed (handlers/query-allowed? '[:find ?e :where [?e :some/unknown-attr]]))))
    (is (false? (:allowed (handlers/query-allowed? '[:find ?x ?y :where [?x :db/id ?y]])))))

(deftest query-allowed?-map-format-test
  (testing "Map format queries are validated correctly"
    (is (true? (:allowed (handlers/query-allowed? '{:find ?e :where [[?e :db/id ?id]]}))))
    (is (false? (:allowed (handlers/query-allowed? '{:find ?e :where [[?e :unknown/attr ?x]]})))))))

(deftest query-allowed?-returns-matched-template-test
  (testing "Allowed queries return matched template ID"
    (let [result (handlers/query-allowed? '[:find ?e :where [?e :db/id ?id]])]
      (is (= :get-entity-by-id (:matched-template result))))
    (let [result2 (handlers/query-allowed? '[:find (count ?e) :where [?e :prov/entity]])]
      (is (= :count-entities (:matched-template result2))))))

;; =============================================================================
;; Query Handler Tests
;; =============================================================================

(deftest handle-query-missing-query-test
  (testing "Missing query parameter returns 400 error"
    (let [response (handlers/handle-query {})
          body (parse-response-body response)]
      (is (= 400 (:status response)))
      (is (re-find #"Missing query" (:error body))))))

(deftest handle-query-not-allowed-test
  (testing "Query not in whitelist returns 403 error"
    (let [request {:body-params {:query '[:find ?e :where [?e :unknown/attr]]}}
          response (handlers/handle-query request)
          body (parse-response-body response)]
      (is (= 403 (:status response)))
      (is (re-find #"not allowed" (:error body))))))

(deftest handle-query-invalid-structure-test
  (testing "Query with invalid structure returns 400 error"
    (let [request {:body-params {:query '[:find ?e]}}
          response (handlers/handle-query request)
          body (parse-response-body response)]
      (is (= 400 (:status response)))
      (is (re-find #"Invalid query format" (:error body))))))

(deftest handle-query-coerces-uuid-string-literals-test
  (testing "UUID literals supplied as JSON strings still match PROV entities"
    (let [seed-response (handlers/handle-load-sample-data {})
          seed-body (parse-response-body seed-response)
          entity-id (get-in seed-body [:data :benchmark-anchors :entity-id])
          request {:body-params {:query {:find "?e"
                                         :where [["?e" ":prov/entity" entity-id]]}}}
          response (handlers/handle-query request)
          body (parse-response-body response)]
      (is (= 200 (:status response)))
      (is (= 1 (get-in body [:data :count])))
      (is (= "get-prov-entities" (get-in body [:data :template-used])))
      (is (= 1 (count (get-in body [:data :results])))))))

;; =============================================================================
;; Graph Handler Tests
;; =============================================================================

(deftest handle-get-entity-invalid-uuid-test
  (testing "Invalid UUID parameter returns error"
    (let [request {:params {:id "not-a-uuid" :depth "2"}}
          response (handlers/handle-get-entity request)]
      (is (= 400 (:status response))))))

(deftest handle-get-entity-not-found-test
  (testing "Non-existent entity returns 404"
    (let [entity-id (UUID/randomUUID)
          request {:params {:id (str entity-id) :depth "2"}}
          response (handlers/handle-get-entity request)]
      (is (= 404 (:status response))))))

(deftest handle-get-entity-success-test
  (testing "Existing PROV entity returns relationships without graph arity errors"
    (let [parent-id (UUID/randomUUID)
          entity-id (UUID/randomUUID)
          _ @(d/transact *conn* [{:db/id "parent"
                                  :prov/entity parent-id
                                  :prov/entity-type :product/raw-material}
                                 {:db/id "entity"
                                  :prov/entity entity-id
                                  :prov/entity-type :product/batch
                                  :prov/wasDerivedFrom parent-id}])
          request {:params {:id (str entity-id) :depth "1"}}
          response (handlers/handle-get-entity request)
          body (parse-response-body response)]
      (is (= 200 (:status response)))
      (is (= 1 (get-in body [:data :depth])))
      (is (= "product/batch" (get-in body [:data :entity :prov/entity-type])))
      (is (seq (get-in body [:data :neighbors :prov/wasDerivedFrom]))))))

(deftest handle-find-path-invalid-uuids-test
  (testing "Invalid UUID parameters return error"
    (let [request {:params {:from "invalid" :to (str (UUID/randomUUID))}}
          response (handlers/handle-find-path request)]
      (is (= 400 (:status response))))))

;; =============================================================================
;; Ontology Handler Tests
;; =============================================================================

(deftest handle-list-ontologies-test
  (testing "List ontologies returns success"
    (let [response (handlers/handle-list-ontologies {})
          body (parse-response-body response)]
      (is (= 200 (:status response)))
      (is (vector? (:ontologies (:data body))))
      (is (number? (:count (:data body)))))))

(deftest handle-get-ontology-invalid-uuid-test
  (testing "Invalid UUID returns error"
    (let [request {:params {:id "not-a-uuid"}}
          response (handlers/handle-get-ontology request)]
      (is (= 400 (:status response))))))

(deftest handle-get-ontology-not-found-test
  (testing "Non-existent ontology returns 404"
    (let [request {:params {:id (str (UUID/randomUUID))}}
          response (handlers/handle-get-ontology request)]
      (is (= 404 (:status response))))))

;; =============================================================================
;; Permission Handler Tests
;; =============================================================================

(deftest handle-check-permission-test
  (testing "Permission check returns result"
    (let [resource-id (str (UUID/randomUUID))
          requestor-id (str (UUID/randomUUID))
          request {:params {:resource-id resource-id
                           :requestor-id requestor-id
                           :action "read"}}
          response (handlers/handle-check-permission request)
          body (parse-response-body response)]
      (is (= 200 (:status response)))
      (is (contains? (:data body) :allowed))
      (= resource-id (get-in body [:data :resource-id]))
      (= requestor-id (get-in body [:data :requestor-id])))))

;; =============================================================================
;; Statistics Handler Tests
;; =============================================================================

(deftest handle-get-stats-test
  (testing "Get stats returns system statistics"
    (let [response (handlers/handle-get-stats {})
          body (parse-response-body response)]
      (is (= 200 (:status response)))
      (is (contains? (:data body) :knowledge-base))
      (is (number? (get-in body [:data :knowledge-base :total-entities])))
      (is (number? (get-in body [:data :knowledge-base :total-activities]))))))

;; =============================================================================
;; Error Handler Tests
;; =============================================================================

(deftest handle-not-found-test
  (testing "404 handler returns not found error"
    (let [response (handlers/handle-not-found {})
          body (parse-response-body response)]
      (is (= 404 (:status response)))
      (is (re-find #"not found" (:error body))))))

(deftest handle-method-not-allowed-test
  (testing "405 handler returns method not allowed error"
    (let [response (handlers/handle-method-not-allowed {})
          body (parse-response-body response)]
      (is (= 405 (:status response)))
      (is (re-find #"not allowed" (:error body))))))

;; =============================================================================
;; Traceability Handler Tests
;; =============================================================================

(deftest handle-trace-product-test
  (testing "Trace product returns response structure"
    (let [product-id "TEST-PRODUCT-001"
          request {:params {:id product-id}}
          response (handlers/handle-trace-product request)
          body (parse-response-body response)]
      (is (= 200 (:status response)))
      (is (contains? (:data body) :product-id))
      (is (contains? (:data body) :history)))))

(deftest handle-get-provenance-invalid-uuid-test
  (testing "Invalid UUID for provenance throws exception"
    (let [request {:params {:id "not-a-uuid"}}]
      (is (thrown? Exception
                   (handlers/handle-get-provenance request))))))

(deftest handle-get-provenance-success-test
  (testing "Valid entity UUID returns provenance tuples"
    (let [entity-id #uuid "550e8400-e29b-41d4-a716-446655440100"
          activity-id #uuid "550e8400-e29b-41d4-a716-446655440101"
          agent-id #uuid "550e8400-e29b-41d4-a716-446655440102"]
      @(d/transact
        *conn*
        [{:prov/entity entity-id
          :prov/entity-type :product/uht-milk
          :prov/wasGeneratedBy activity-id}
         {:prov/activity activity-id
          :prov/activity-type :activity/processing
          :prov/startedAtTime #inst "2024-01-15T08:30:00.000-00:00"
          :prov/wasAssociatedWith [agent-id]}
         {:prov/agent agent-id
          :prov/agent-type :organization/manufacturer
          :prov/agent-name "Northern Thai UHT Processing Ltd."}])
      (let [response (handlers/handle-get-provenance
                      {:params {:id (str entity-id)}})
            body (parse-response-body response)]
        (is (= 200 (:status response)))
        (is (= (str entity-id) (get-in body [:data :entity-id])))
        (is (= 1 (count (get-in body [:data :provenance]))))))))

(deftest handle-get-timeline-invalid-uuid-test
  (testing "Invalid UUID for timeline throws exception"
    (let [request {:params {:id "not-a-uuid"}}]
      (is (thrown? Exception
                   (handlers/handle-get-timeline request))))))

;; =============================================================================
;; With-Connection Macro Tests
;; =============================================================================

(deftest with-connection-binds-connection-test
  (testing "with-connection macro binds *connection*"
    (let [test-conn (Object.)]
      (handlers/with-connection test-conn
        (is (identical? test-conn handlers/*connection*))))))

;; =============================================================================
;; Cluster Consensus Handler Tests
;; =============================================================================

(deftest verify-node-auth-no-header-test
  (testing "Request without X-Node-ID header returns nil"
    (let [request {:headers {}}
          result (handlers/verify-node-auth request)]
      (is (nil? result)))))

(deftest verify-node-auth-invalid-node-test
  (testing "Request with unknown node ID returns nil"
    (let [request {:headers {"x-node-id" "unknown-node"}}
          result (handlers/verify-node-auth request)]
      (is (nil? result)))))

(deftest handle-internal-cluster-status-disabled-test
  (testing "Cluster status returns disabled when cluster not enabled"
    (let [request {}
          response (handlers/handle-internal-cluster-status request)
          body (parse-response-body response)]
      (is (= 200 (:status response)))
      (is (false? (get-in body [:data :cluster-enabled]))))))

;; =============================================================================
;; Transaction API Tests
;; =============================================================================

(deftest handle-submit-transaction-no-cluster-test
  (testing "Submit transaction fails when cluster not enabled"
    (let [request {:body-params {:entity-id (str (UUID/randomUUID))
                                 :entity-type "product/batch"}}
          response (handlers/handle-submit-transaction request)
          body (parse-response-body response)]
      (is (= 503 (:status response)))
      (is (re-find #"Cluster mode not enabled" (:error body))))))

(deftest handle-submit-transaction-missing-fields-test
  (testing "Submit transaction with missing fields returns error"
    (let [request {:body-params {:entity-id (str (UUID/randomUUID))}}
          response (handlers/handle-submit-transaction request)]
      (is (= 503 (:status response))))))

(deftest handle-transaction-status-test
  (testing "Transaction status returns response for unknown transaction"
    (let [tx-id "unknown-proposal-id"
          request {:params {:id tx-id}}
          response (handlers/handle-transaction-status request)
          body (parse-response-body response)]
      (is (= 200 (:status response)))
      (is (= tx-id (get-in body [:data :transaction-id])))
      (is (= :not-found (get-in body [:data :status])))))))

(deftest handle-list-pending-transactions-test
  (testing "List pending transactions returns empty when no proposals"
    (let [request {}
          mock-connection nil  ;; No connection needed for empty list
          response (handlers/handle-list-pending-transactions request mock-connection)
          body (parse-response-body response)]
      (is (= 200 (:status response)))
      (is (= 0 (get-in body [:data :count])))
      (is (vector? (get-in body [:data :transactions]))))))

;; max-page-size and default-page-size are private constants
;; They are implementation details, not part of the public API
