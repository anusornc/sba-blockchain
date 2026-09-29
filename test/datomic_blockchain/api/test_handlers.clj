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
            [datomic-blockchain.api.handlers.common :as common]
            [datomic-blockchain.api.handlers.graph :as graph]
            [datomic-blockchain.api.handlers.traceability :as traceability]
            [datomic-blockchain.api.handlers.query :as query]
            [datomic-blockchain.api.handlers.blocks :as blocks]
            [datomic-blockchain.api.handlers.ontology :as ontology]
            [datomic-blockchain.api.handlers.permission :as permission]
            [datomic-blockchain.api.handlers.transactions :as transactions]
            [datomic-blockchain.api.handlers.cluster :as cluster]
            [datomic-blockchain.api.handlers.dev :as dev]
            [datomic-blockchain.api.routes :as routes]
            [datomic-blockchain.test-support :as ts]
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

(def ^:private test-conn (atom nil))
(def ^:private test-policy (atom nil))

(defn- temp-conn-fixture
  "Fresh in-memory database per test via the test-support seam."
  [f]
  (reset! test-conn (ts/fresh-conn))
  (reset! test-policy (policy/init-policy-store (model/visibility-strategy)))
  (f)
  (ts/retire-all!))

(defn setup-policy-store
  "Initialize the policy store before permission tests"
  [f]
  (reset! test-policy (policy/init-policy-store (model/visibility-strategy)))
  (f))

(use-fixtures :each temp-conn-fixture setup-policy-store)

;; =============================================================================
;; Input Validation Tests
;; =============================================================================

(deftest valid-uuid?-test
  (testing "Valid UUID formats are accepted"
    (is (true? (common/valid-uuid? "550e8400-e29b-41d4-a716-446655440000")))
    (is (true? (common/valid-uuid? "00000000-0000-0000-0000-000000000000")))
    (is (true? (common/valid-uuid? "FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF")))))

(deftest valid-uuid?-rejects-invalid-test
  (testing "Invalid UUID formats are rejected"
    (is (false? (common/valid-uuid? "not-a-uuid")))
    (is (false? (common/valid-uuid? "550e8400-e29b-41d4-a716")))
    (is (false? (common/valid-uuid? "")))
    (is (false? (common/valid-uuid? nil)))
    (is (false? (common/valid-uuid? 123)))))

(deftest parse-uuid-safe-test
  (testing "Valid UUID strings are parsed to UUID objects"
    (let [uuid-str "550e8400-e29b-41d4-a716-446655440000"
          parsed (common/parse-uuid-safe uuid-str)]
      (is (instance? UUID parsed))
      (is (= uuid-str (str parsed)))))
  (testing "Invalid UUID strings return nil"
    (is (nil? (common/parse-uuid-safe "invalid")))
    (is (nil? (common/parse-uuid-safe nil)))))

(deftest validate-uuid-param-valid-test
  (testing "Valid UUID parameter returns UUID object"
    (let [uuid-str "550e8400-e29b-41d4-a716-446655440000"
          result (common/validate-uuid-param :id uuid-str)]
      (is (instance? UUID result))
      (is (= uuid-str (str result))))))

(deftest validate-uuid-param-invalid-format-test
  (testing "Invalid UUID format throws ex-info"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Invalid UUID format"
                          (common/validate-uuid-param :id "not-a-uuid")))))

(deftest validate-positive-int-valid-test
  (testing "Valid positive integers are parsed"
    (is (= 10 (common/validate-positive-int :page "10" 10)))
    (is (= 0 (common/validate-positive-int :page "0" 10)))
    (is (= 5 (common/validate-positive-int :page nil 5)))))

(deftest validate-positive-int-invalid-test
  (testing "Invalid integers throw ex-info"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Invalid integer"
                          (common/validate-positive-int :page "abc" 10)))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"must be non-negative"
                          (common/validate-positive-int :page "-1" 10)))))

(deftest sanitize-string-param-valid-test
  (testing "Valid strings are returned as-is"
    (is (= "hello" (common/sanitize-string-param :name "hello" 100)))
    (is (= "test123" (common/sanitize-string-param :name "test123" 100)))))

(deftest sanitize-string-param-max-length-test
  (testing "Strings exceeding max length throw ex-info"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Parameter too long"
                          (common/sanitize-string-param :name (apply str (repeat 101 "a")) 100)))))

(deftest sanitize-string-param-suspicious-patterns-test
  (testing "Suspicious patterns throw ex-info"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Suspicious input"
                          (common/sanitize-string-param :name "<script>alert('xss')</script>" 100)))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Suspicious input"
                          (common/sanitize-string-param :name "javascript:alert(1)" 100)))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Suspicious input"
                          (common/sanitize-string-param :name "test onerror=bad" 100)))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Suspicious input"
                          (common/sanitize-string-param :name "test onload=bad" 100)))))

(deftest sanitize-string-param-nil-test
  (testing "Nil values return nil"
    (is (nil? (common/sanitize-string-param :name nil 100)))))

;; =============================================================================
;; Health Check Tests
;; =============================================================================

(deftest handle-health-test
  (testing "Health check returns healthy status"
    (let [response (routes/handle-health {})
          body (parse-response-body response)]
      (is (= 200 (:status response)))
      (is (= "healthy" (get-in body [:data :status]))))))

(deftest handle-load-sample-data-benchmark-anchors-test
  (testing "Sample-data seed endpoint returns benchmark anchors and relationship count"
    (let [response (dev/handle-load-sample-data {} @test-conn)
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
      (is (some? (common/parse-uuid-safe (:entity-id anchors))))
      (is (some? (common/parse-uuid-safe (:activity-id anchors))))
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
    (is (true? (:allowed (query/query-allowed? '[:find ?e :where [?e :db/id ?id]]))))
    ;; get-prov-entities
    (is (true? (:allowed (query/query-allowed? '[:find ?e :where [?e :prov/entity ?entity-id]]))))
    ;; get-prov-activities
    (is (true? (:allowed (query/query-allowed? '[:find ?a :where [?a :prov/activity ?activity-id]]))))
    ;; get-prov-agents
    (is (true? (:allowed (query/query-allowed? '[:find ?ag :where [?ag :prov/agent ?agent-id]]))))
    ;; count-entities
    (is (true? (:allowed (query/query-allowed? '[:find (count ?e) :where [?e :prov/entity]]))))))

(deftest query-allowed?-rejected-queries-test
  (testing "Queries not in whitelist are rejected"
    (is (false? (:allowed (query/query-allowed? '[:find ?e :where [?e :some/unknown-attr]]))))
    (is (false? (:allowed (query/query-allowed? '[:find ?x ?y :where [?x :db/id ?y]])))))

(deftest query-allowed?-map-format-test
  (testing "Map format queries are validated correctly"
    (is (true? (:allowed (query/query-allowed? '{:find ?e :where [[?e :db/id ?id]]}))))
    (is (false? (:allowed (query/query-allowed? '{:find ?e :where [[?e :unknown/attr ?x]]})))))))

(deftest query-allowed?-returns-matched-template-test
  (testing "Allowed queries return matched template ID"
    (let [result (query/query-allowed? '[:find ?e :where [?e :db/id ?id]])]
      (is (= :get-entity-by-id (:matched-template result))))
    (let [result2 (query/query-allowed? '[:find (count ?e) :where [?e :prov/entity]])]
      (is (= :count-entities (:matched-template result2))))))

;; =============================================================================
;; Query Handler Tests
;; =============================================================================

(deftest handle-query-missing-query-test
  (testing "Missing query parameter returns 400 error"
    (let [response (query/handle-query {} @test-conn)
          body (parse-response-body response)]
      (is (= 400 (:status response)))
      (is (re-find #"Missing query" (:error body))))))

(deftest handle-query-not-allowed-test
  (testing "Query not in whitelist returns 403 error"
    (let [request {:body-params {:query '[:find ?e :where [?e :unknown/attr]]}}
          response (query/handle-query request @test-conn)
          body (parse-response-body response)]
      (is (= 403 (:status response)))
      (is (re-find #"not allowed" (:error body))))))

(deftest handle-query-invalid-structure-test
  (testing "Query with invalid structure returns 400 error"
    (let [request {:body-params {:query '[:find ?e]}}
          response (query/handle-query request @test-conn)
          body (parse-response-body response)]
      (is (= 400 (:status response)))
      (is (re-find #"Invalid query format" (:error body))))))

(deftest handle-query-coerces-uuid-string-literals-test
  (testing "UUID literals supplied as JSON strings still match PROV entities"
    (let [seed-response (dev/handle-load-sample-data {} @test-conn)
          seed-body (parse-response-body seed-response)
          entity-id (get-in seed-body [:data :benchmark-anchors :entity-id])
          request {:body-params {:query {:find "?e"
                                         :where [["?e" ":prov/entity" entity-id]]}}}
          response (query/handle-query request @test-conn)
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
          response (graph/handle-get-entity request @test-conn)]
      (is (= 400 (:status response))))))

(deftest handle-get-entity-not-found-test
  (testing "Non-existent entity returns 404"
    (let [entity-id (UUID/randomUUID)
          request {:params {:id (str entity-id) :depth "2"}}
          response (graph/handle-get-entity request @test-conn)]
      (is (= 404 (:status response))))))

(deftest handle-get-entity-success-test
  (testing "Existing PROV entity returns relationships without graph arity errors"
    (let [parent-id (UUID/randomUUID)
          entity-id (UUID/randomUUID)
          _ @(d/transact @test-conn [{:db/id "parent"
                                  :prov/entity parent-id
                                  :prov/entity-type :product/raw-material}
                                 {:db/id "entity"
                                  :prov/entity entity-id
                                  :prov/entity-type :product/batch
                                  :prov/wasDerivedFrom parent-id}])
          request {:params {:id (str entity-id) :depth "1"}}
          response (graph/handle-get-entity request @test-conn)
          body (parse-response-body response)]
      (is (= 200 (:status response)))
      (is (= 1 (get-in body [:data :depth])))
      (is (= "batch" (get-in body [:data :entity :entity-type])))
      (is (seq (get-in body [:data :neighbors :wasDerivedFrom]))))))

(deftest handle-find-path-invalid-uuids-test
  (testing "Invalid UUID parameters return error"
    (let [request {:params {:from "invalid" :to (str (UUID/randomUUID))}}
          response (graph/handle-find-path request @test-conn)]
      (is (= 400 (:status response))))))

;; =============================================================================
;; Ontology Handler Tests
;; =============================================================================

(deftest handle-list-ontologies-test
  (testing "List ontologies returns success"
    (let [response (ontology/handle-list-ontologies {} @test-conn)
          body (parse-response-body response)]
      (is (= 200 (:status response)))
      (is (vector? (:ontologies (:data body))))
      (is (number? (:count (:data body)))))))

(deftest handle-get-ontology-invalid-uuid-test
  (testing "Invalid UUID returns error"
    (let [request {:params {:id "not-a-uuid"}}
          response (ontology/handle-get-ontology request @test-conn)]
      (is (= 400 (:status response))))))

(deftest handle-get-ontology-not-found-test
  (testing "Non-existent ontology returns 404"
    (let [request {:params {:id (str (UUID/randomUUID))}}
          response (ontology/handle-get-ontology request @test-conn)]
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
          response (permission/handle-check-permission request @test-conn @test-policy)
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
    (let [response (graph/handle-get-stats {} @test-conn)
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
    (let [response (routes/handle-not-found {})
          body (parse-response-body response)]
      (is (= 404 (:status response)))
      (is (re-find #"not found" (:error body))))))

(deftest handle-method-not-allowed-test
  (testing "405 handler returns method not allowed error"
    (let [response (routes/handle-method-not-allowed {})
          body (parse-response-body response)]
      (is (= 405 (:status response)))
      (is (re-find #"not allowed" (:error body))))))

;; =============================================================================
;; Traceability Handler Tests
;; =============================================================================

(deftest handle-trace-product-test
  (testing "Trace product returns response structure"
    (let [seed-body (parse-response-body (dev/handle-load-sample-data {} @test-conn))
          entity-id (get-in seed-body [:data :benchmark-anchors :entity-id])
          request {:params {:id entity-id}}
          response (traceability/handle-trace-product request @test-conn)
          body (parse-response-body response)]
      (is (= 200 (:status response)))
      (is (contains? (:data body) :product-id))
      (is (contains? (:data body) :history))))
  (testing "Unknown product id is a 404"
    (let [response (traceability/handle-trace-product
                    {:params {:id "NO-SUCH-PRODUCT"}} @test-conn)]
      (is (= 404 (:status response))))))

(deftest handle-ui-trace-qr-test
  (testing "UI trace contract normalizes deterministic QR trace data"
    (let [response (traceability/handle-ui-trace
                    {:params {:code "UHT-CHOC-2024-001-QR"
                              :kind "qr"}}
                    @test-conn)
          body (parse-response-body response)
          data (:data body)
          nodes (get-in data [:graph :nodes])
          edges (get-in data [:graph :edges])
          node-ids (set (map :id nodes))]
      (is (= 200 (:status response)))
      (is (true? (:success body)))
      (is (= "2026-10-01" (:contract-version data)))
      (is (= "deterministic-uht-dataset" (:source data)))
      (is (= "qr" (get-in data [:query :kind])))
      (is (= "UHT-CHOC-2024-001-QR" (get-in data [:product :qr-code])))
      (is (= 4 (count (:stages data))))
      (is (= (count nodes) (count node-ids))
          "UI graph should not duplicate product nodes")
      (is (contains? node-ids "UHT-CHOC-CM-2024-001"))
      (is (contains? node-ids "UHT-CHOC-2024-001-QR"))
      (is (some #(and (= "UHT-CHOC-CM-2024-001" (:source %))
                      (= "farm" (:target %))
                      (= "prov:wasGeneratedBy" (:label %)))
                edges))
      (is (every? #(contains? % :source) edges))
      (is (every? #(contains? % :target) edges)))))

(deftest handle-ui-trace-batch-test
  (testing "UI trace contract supports batch lookup"
    (let [response (traceability/handle-ui-trace
                    {:params {:code "UHT-PLAIN-CM-2024-001"
                              :kind "batch"}}
                    @test-conn)
          body (parse-response-body response)]
      (is (= 200 (:status response)))
      (is (= "batch" (get-in body [:data :query :kind])))
      (is (= "UHT-PLAIN-CM-2024-001" (get-in body [:data :product :batch]))))))

(deftest handle-ui-trace-invalid-kind-test
  (testing "UI trace contract rejects unsupported lookup kinds"
    (let [response (traceability/handle-ui-trace
                    {:params {:code "UHT-CHOC-2024-001-QR"
                              :kind "entity"}}
                    @test-conn)
          body (parse-response-body response)]
      (is (= 400 (:status response)))
      (is (= false (:success body)))
      (is (= "unsupported-trace-kind" (get-in body [:details :error]))))))

(deftest handle-ui-ontology-test
  (testing "UI ontology contract returns legend vocabulary"
    (let [response (traceability/handle-ui-ontology {} @test-conn)
          body (parse-response-body response)
          data (:data body)]
      (is (= 200 (:status response)))
      (is (= "2026-10-01" (:contract-version data)))
      (is (= "http://www.w3.org/ns/prov#" (get-in data [:prefixes :prov])))
      (is (some #(= "ProductBatch" (:id %)) (:classes data)))
      (is (some #(= "wasGeneratedBy" (:id %)) (:properties data))))))

(deftest handle-get-provenance-invalid-uuid-test
  (testing "Invalid UUID for provenance returns a 400 error envelope"
    (let [request {:params {:id "not-a-uuid"}}
          response (traceability/handle-get-provenance request @test-conn)]
      (is (= 400 (:status response)))
      (is (re-find #"must be a valid UUID" (:body response)))
      (is (re-find #"\"success\":false" (:body response))))))

(deftest handle-get-provenance-success-test
  (testing "Valid entity UUID returns provenance tuples"
    (let [entity-id #uuid "550e8400-e29b-41d4-a716-446655440100"
          activity-id #uuid "550e8400-e29b-41d4-a716-446655440101"
          agent-id #uuid "550e8400-e29b-41d4-a716-446655440102"]
      @(d/transact
        @test-conn
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
      (let [response (traceability/handle-get-provenance
                      {:params {:id (str entity-id)}}
                      @test-conn)
            body (parse-response-body response)]
        (is (= 200 (:status response)))
        (is (= (str entity-id) (get-in body [:data :entity-id])))
        (is (= 1 (count (get-in body [:data :provenance]))))))))

(deftest handle-get-timeline-invalid-uuid-test
  (testing "Invalid UUID for timeline throws exception"
    (let [request {:params {:id "not-a-uuid"}}
          response (traceability/handle-get-timeline request @test-conn)]
      (is (= 400 (:status response)))
      (is (re-find #"must be a valid UUID" (:body response)))
      (is (re-find #"\"success\":false" (:body response))))))

;; =============================================================================
;; With-Connection Macro Tests
;; =============================================================================

;; =============================================================================
;; Cluster Consensus Handler Tests
;; =============================================================================

(deftest verify-node-auth-no-header-test
  (testing "Request without X-Node-ID header returns nil"
    (let [request {:headers {}}
          result (cluster/verify-node-auth request)]
      (is (nil? result)))))

(deftest verify-node-auth-invalid-node-test
  (testing "Request with unknown node ID returns nil"
    (let [request {:headers {"x-node-id" "unknown-node"}}
          result (cluster/verify-node-auth request)]
      (is (nil? result)))))

(deftest handle-internal-cluster-status-disabled-test
  (testing "Cluster status returns disabled when cluster not enabled"
    (let [request {}
          response (cluster/handle-internal-cluster-status request @test-conn)
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
          response (transactions/handle-submit-transaction request @test-conn)
          body (parse-response-body response)]
      (is (= 503 (:status response)))
      (is (re-find #"Cluster mode not enabled" (:error body))))))

(deftest handle-submit-transaction-missing-fields-test
  (testing "Submit transaction with missing fields returns error"
    (let [request {:body-params {:entity-id (str (UUID/randomUUID))}}
          response (transactions/handle-submit-transaction request @test-conn)]
      (is (= 503 (:status response))))))

(deftest handle-transaction-status-test
  (testing "Transaction status returns response for unknown transaction"
    (let [tx-id "unknown-proposal-id"
          request {:params {:id tx-id}}
          response (transactions/handle-transaction-status request @test-conn)
          body (parse-response-body response)]
      (is (= 200 (:status response)))
      (is (= tx-id (get-in body [:data :transaction-id])))
      (is (= "not-found" (get-in body [:data :status]))))))

(deftest handle-list-pending-transactions-test
  (testing "List pending transactions returns empty when no proposals"
    (let [request {}
          mock-connection nil  ;; No connection needed for empty list
          response (transactions/handle-list-pending-transactions request mock-connection)
          body (parse-response-body response)]
      (is (= 200 (:status response)))
      (is (= 0 (get-in body [:data :count])))
      (is (vector? (get-in body [:data :transactions]))))))

;; =============================================================================
;; Live UHT demo path (sample load → QR / graph / trace / stats / blocks)
;; =============================================================================

(defn- with-fresh-conn
  "Run f with a dedicated in-memory database via the test-support seam."
  [f]
  (ts/with-fresh-db [conn]
    (f conn)))

(defn- load-uht-sample
  "Load the UHT sample dataset and return parsed response data."
  [conn]
  (parse-response-body (dev/handle-load-sample-data {} conn)))

(deftest uht-demo-ontologies-and-stats-after-sample-load-test
  (testing "Ontologies and stats succeed after sample load"
    (with-fresh-conn
      (fn [conn]
        (let [_ (load-uht-sample conn)
              ontologies-resp (ontology/handle-list-ontologies {} conn)
              stats-resp (graph/handle-get-stats {} conn)
              ontologies (parse-response-body ontologies-resp)
              stats (parse-response-body stats-resp)]
          (is (= 200 (:status ontologies-resp)))
          (is (true? (:success ontologies)))
          (is (vector? (get-in ontologies [:data :ontologies])))
          (is (number? (get-in ontologies [:data :count])))
          (is (number? (get-in stats [:data :knowledge-base :total-entities])))
          (is (number? (get-in stats [:data :knowledge-base :total-activities])))
          (is (= 4 (get-in stats [:data :knowledge-base :total-entities])))
          (is (= 7 (get-in stats [:data :knowledge-base :total-activities]))))))))

(deftest uht-demo-graph-after-sample-load-test
  (testing "Graph endpoints return nodes for the chocolate entity"
    (with-fresh-conn
      (fn [conn]
        (let [sample (load-uht-sample conn)
              entity-id (get-in sample [:data :benchmark-anchors :entity-id])
              graph-resp (graph/handle-get-graph {:params {:id entity-id}} conn)
              entity-resp (graph/handle-get-entity {:params {:id entity-id}} conn)
              graph (parse-response-body graph-resp)
              entity (parse-response-body entity-resp)]
          (is (= 200 (:status graph-resp)))
          (is (= 200 (:status entity-resp)))
          (is (seq (get-in graph [:data :nodes])))
          (is (contains? (:data entity) :entity))
          (is (map? (get-in entity [:data :neighbors]))))))))

(deftest uht-demo-qr-reads-datomic-test
  (testing "QR lookup uses loaded ledger data and unique graph node ids"
    (with-fresh-conn
      (fn [conn]
        (let [_ (load-uht-sample conn)
              response (traceability/handle-trace-by-qr
                        {:params {:qr "UHT-CHOC-2024-001-QR"}}
                        conn)
              body (parse-response-body response)
              node-ids (map :id (get-in body [:data :graph :nodes]))]
          (is (= 200 (:status response)))
          (is (= "UHT-CHOC-CM-2024-001" (get-in body [:data :product :batch])))
          (is (= "UHT-CHOC-2024-001-QR" (get-in body [:data :product :qr-code])))
          (is (seq (get-in body [:data :journey :stages])))
          (is (= (count node-ids) (count (set node-ids)))))))))

(deftest uht-qr-and-trace-include-completeness-score-test
  (testing "QR interface and trace return completeness score and missing evidence"
    (with-fresh-conn
      (fn [conn]
        (let [sample (load-uht-sample conn)
              entity-id (get-in sample [:data :benchmark-anchors :entity-id])
              qr-resp (traceability/handle-trace-by-qr
                       {:params {:qr "UHT-CHOC-2024-001-QR"}}
                       conn)
              trace-resp (traceability/handle-trace-product {:params {:id entity-id}} conn)
              qr (parse-response-body qr-resp)
              trace (parse-response-body trace-resp)
              qr-score (get-in qr [:data :completeness-score])
              trace-score (get-in trace [:data :completeness-score])]
          (is (= 200 (:status qr-resp)))
          (is (= 200 (:status trace-resp)))
          (is (true? (:success qr)))
          (is (number? qr-score))
          (is (<= 0 qr-score 1))
          (is (vector? (get-in qr [:data :missing-evidence])))
          (is (number? trace-score))
          (is (<= 0 trace-score 1))
          (is (vector? (get-in trace [:data :missing-evidence])))
          (is (= qr-score trace-score)))))))

(deftest uht-demo-trace-provenance-timeline-named-test
  (testing "Trace/provenance/timeline return named events from Datomic"
    (with-fresh-conn
      (fn [conn]
        (let [sample (load-uht-sample conn)
              entity-id (get-in sample [:data :benchmark-anchors :entity-id])
              trace (parse-response-body
                     (traceability/handle-trace-product {:params {:id entity-id}} conn))
              provenance (parse-response-body
                          (traceability/handle-get-provenance {:params {:id entity-id}} conn))
              timeline (parse-response-body
                        (traceability/handle-get-timeline {:params {:id entity-id}} conn))
              first-prov (first (get-in provenance [:data :provenance]))
              first-event (first (get-in trace [:data :history]))]
          (is (pos? (get-in trace [:data :events])))
          (is (seq (get-in trace [:data :history])))
          (is (map? first-event))
          (is (string? (or (:activity-type first-event)
                           (:activity-name first-event)
                           (get first-event (keyword "activity-type")))))
          (is (pos? (count (get-in provenance [:data :provenance]))))
          (is (map? first-prov))
          (is (string? (or (:agent-name first-prov)
                           (:entity-name first-prov))))
          (is (pos? (get-in timeline [:data :event-count])))
          (is (map? (first (get-in timeline [:data :timeline])))))))))

(deftest uht-demo-create-test-blocks-lists-all-test
  (testing "Creating three test blocks lists three blocks"
    (with-fresh-conn
      (fn [conn]
        (let [_ (dev/handle-create-test-blocks {:params {"count" "3"}} conn)
              response (blocks/handle-list-blocks {:params {}} conn)
              body (parse-response-body response)]
          (is (= 200 (:status response)))
          (is (= 3 (get-in body [:data :pagination :total])))
          (is (= 3 (count (get-in body [:data :blocks])))))))))

;; max-page-size and default-page-size are private constants
;; They are implementation details, not part of the public API
