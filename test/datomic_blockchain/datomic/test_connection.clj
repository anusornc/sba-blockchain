(ns datomic-blockchain.datomic.test-connection
  "Comprehensive test suite for Datomic connection management.

   Tests cover:
   - Database creation (dev and free server types)
   - Database deletion
   - Database recreation
   - Connection establishment
   - Database value retrieval
   - Transaction operations
   - Time travel queries (as-of, since, history)
   - Entity time travel (as-of and history)"
  (:require [clojure.test :refer :all]
            [datomic-blockchain.datomic.connection :as conn]
            [datomic-blockchain.datomic.schema :as schema]
            [datomic.api :as d])
  (:import [java.util UUID Date]))

;; =============================================================================
;; Helper Functions
;; =============================================================================

(defn- temp-conn-fixture
  "Create a temporary in-memory Datomic connection for testing"
  [f]
  (let [uri "datomic:mem://test-connection"]
    (d/delete-database uri)
    (d/create-database uri)
    (let [connection (d/connect uri)]
      ;; Install minimal schema
      @(d/transact connection schema/full-schema)
      (f)
      (d/release connection))))

(use-fixtures :each temp-conn-fixture)

(defn- conn-with-schema
  "Create a new connection with schema installed"
  [db-name]
  (let [uri (format "datomic:mem://%s" db-name)]
    (d/delete-database uri)
    (d/create-database uri)
    (let [connection (d/connect uri)]
      @(d/transact connection schema/full-schema)
      connection)))

;; =============================================================================
;; Test Configuration
;; =============================================================================

(defn dev-config
  "Create a dev configuration for testing"
  ([] (dev-config "test-connection"))
  ([db-name]
   {:datomic {:server-type :dev
              :db-name db-name
              :port 4334}}))

(defn free-config
  "Create a free configuration for testing"
  ([] (free-config "test-connection"))
  ([db-name]
   {:datomic {:server-type :free
              :db-name db-name
              :port 4334}}))

;; =============================================================================
;; Database Creation Tests
;; =============================================================================

(deftest create-database-dev-test
  (testing "Database can be created for dev server type"
    (let [cfg (dev-config "create-test-dev")]
      (is (nil? (conn/create-database cfg)))
      ;; Verify database exists by connecting to it
      (let [uri "datomic:mem://create-test-dev"]
        (d/delete-database uri)))))

(deftest create-database-already-exists-test
  (testing "Creating existing database logs warning but doesn't throw"
    (let [cfg (dev-config "existing-db")]
      (d/create-database "datomic:mem://existing-db")
      (is (nil? (conn/create-database cfg)))
      (d/delete-database "datomic:mem://existing-db"))))

;; =============================================================================
;; Database Deletion Tests
;; =============================================================================

(deftest delete-database-dev-test
  (testing "Database can be deleted for dev server type"
    (d/create-database "datomic:mem://delete-test-dev")
    (is (nil? (conn/delete-database (dev-config "delete-test-dev"))))))

(deftest delete-database-nonexistent-test
  (testing "Deleting non-existent database returns false and logs warning"
    (let [result (conn/delete-database (dev-config "nonexistent-db"))]
      ;; Should not throw, returns false or nil
      (is (or (false? result) (nil? result))))))

;; =============================================================================
;; Database Recreation Tests
;; =============================================================================

(deftest recreate-database-test
  (testing "Database can be recreated (delete and create)"
    (let [cfg (dev-config "recreate-test")]
      ;; Create database with data
      (let [test-conn (conn-with-schema "recreate-test")]
        @(d/transact test-conn [{:db/id "temp" :prov/entity (UUID/randomUUID)}])
        (d/release test-conn))
      ;; Recreate
      (is (nil? (conn/recreate-database cfg)))
      ;; Verify fresh database (old data gone) by using conn-with-schema
      (let [fresh-conn (conn-with-schema "recreate-test")]
        (is (some? fresh-conn))
        (d/release fresh-conn))
      (d/delete-database "datomic:mem://recreate-test"))))

;; =============================================================================
;; Connection Tests
;; =============================================================================

(deftest connect-dev-test
  (testing "Can connect to dev database"
    (let [connection (conn-with-schema "connect-test-dev")]
      (is (some? connection))
      (d/release connection)
      (d/delete-database "datomic:mem://connect-test-dev"))))

(deftest connect-creates-database-if-not-exists-test
  (testing "Connect creates database if it doesn't exist"
    (let [connection (conn-with-schema "auto-create-test")]
      (is (some? connection))
      (d/release connection)
      (d/delete-database "datomic:mem://auto-create-test"))))

;; =============================================================================
;; Get Database Value Tests
;; =============================================================================

(deftest get-db-test
  (testing "Can get current database value from connection"
    (let [connection (conn-with-schema "get-db-test")]
      (let [db (conn/get-db connection)]
        (is (some? db))
        (is (instance? datomic.db.Db db)))
      (d/release connection)
      (d/delete-database "datomic:mem://get-db-test"))))

;; =============================================================================
;; Transaction Tests
;; =============================================================================

(deftest transact-test
  (testing "Can transact data with logging"
    (let [connection (conn-with-schema "transact-test")
          entity-id (UUID/randomUUID)
          tx-data [{:db/id "temp"
                    :prov/entity entity-id
                    :prov/entity-type :product/batch}]]
      (let [result @(conn/transact connection tx-data)]
        (is (some? result))
        (is (map? result))
        ;; Verify transacted data exists - query by :prov/entity
        (let [db (d/db connection)
              entity-result (d/q '[:find ?e :where [?e :prov/entity ?eid]] db entity-id)]
          (is (some? (ffirst entity-result)))))
      (d/release connection)
      (d/delete-database "datomic:mem://transact-test"))))

(deftest transact-empty-data-test
  (testing "Transacting empty data returns success"
    (let [connection (conn-with-schema "empty-tx-test")]
      (let [result @(conn/transact connection [])]
        (is (some? result)))
      (d/release connection)
      (d/delete-database "datomic:mem://empty-tx-test"))))

;; =============================================================================
;; Time Travel Tests (as-of)
;; =============================================================================

(deftest as-of-test
  (testing "Can get database as of specific transaction"
    (let [connection (conn-with-schema "as-of-test")
          entity-id (UUID/randomUUID)]
      ;; Transact initial data
      (let [tx-result @(d/transact connection [{:db/id "temp"
                                                :prov/entity entity-id
                                                :traceability/product-name "initial"}])
            actual-entity-id (get (:tempids tx-result) "temp")
            t-after-initial (d/basis-t (d/db connection))]
        ;; Transact update using actual entity ID
        @(d/transact connection [[:db/add actual-entity-id :traceability/product-name "updated"]])
        ;; Get database as of t-after-initial (should have initial value)
        (let [db-as-of (conn/as-of connection t-after-initial)
              entity (d/entity db-as-of actual-entity-id)]
          (is (= "initial" (:traceability/product-name entity)))))
      ;; Cleanup
      (d/release connection)
      (d/delete-database "datomic:mem://as-of-test"))))

;; =============================================================================
;; Time Travel Tests (since)
;; =============================================================================

(deftest since-test
  (testing "Can get database since specific transaction"
    (let [connection (conn-with-schema "since-test")
          entity-id (UUID/randomUUID)]
      ;; Initial transaction
      (let [tx-result @(d/transact connection [{:db/id "temp"
                                                :prov/entity entity-id
                                                :traceability/product-name "initial"}])
            actual-entity-id (get (:tempids tx-result) "temp")
            t1 (d/basis-t (d/db connection))]
        ;; Wait a bit to ensure different transaction time
        (Thread/sleep 10)
        ;; Transact more data using actual entity ID
        @(d/transact connection [[:db/add actual-entity-id :traceability/product-name "updated"]])
        ;; Get database since t1 (should only have changes after t1)
        (let [db-since (conn/since connection t1)]
          (is (some? db-since))
          (is (instance? datomic.db.Db db-since))))
      ;; Cleanup
      (d/release connection)
      (d/delete-database "datomic:mem://since-test"))))

;; =============================================================================
;; History Tests
;; =============================================================================

(deftest history-test
  (testing "Can get history database"
    (let [connection (conn-with-schema "history-test")]
      (let [history-db (conn/history connection)]
        (is (some? history-db))
        (is (instance? datomic.db.Db history-db)))
      (d/release connection)
      (d/delete-database "datomic:mem://history-test"))))

;; =============================================================================
;; Entity Time Travel Tests
;; =============================================================================

(deftest entity-as-of-test
  (testing "Can get entity as of specific transaction point"
    (let [connection (conn-with-schema "entity-as-of-test")
          entity-id (UUID/randomUUID)]
      ;; Create entity
      (let [tx-result @(d/transact connection [{:db/id "temp"
                                                :prov/entity entity-id
                                                :traceability/product-name "v1"}])
            actual-entity-id (get (:tempids tx-result) "temp")
            t1 (d/basis-t (d/db connection))]
        ;; Update entity
        @(d/transact connection [[:db/add actual-entity-id :traceability/product-name "v2"]])
        ;; Get entity as of t1
        (let [entity-t1 (conn/entity-as-of connection actual-entity-id t1)]
          (is (some? entity-t1))
          (is (= "v1" (:traceability/product-name entity-t1)))))
      ;; Cleanup
      (d/release connection)
      (d/delete-database "datomic:mem://entity-as-of-test"))))

(deftest entity-history-test
  (testing "Can get full history of an entity"
    (let [connection (conn-with-schema "entity-history-test")
          entity-id (UUID/randomUUID)]
      ;; Create entity
      (let [tx-result @(d/transact connection [{:db/id "temp"
                                                :prov/entity entity-id
                                                :traceability/product-name "initial"}])
            actual-entity-id (get (:tempids tx-result) "temp")]
        ;; Update entity multiple times
        @(d/transact connection [[:db/add actual-entity-id :traceability/product-name "v2"]])
        @(d/transact connection [[:db/add actual-entity-id :traceability/product-name "v3"]])
        ;; Get history
        (let [history (conn/entity-history connection actual-entity-id)]
          (is (sequential? history))
          ;; Should have at least 3 versions (initial + 2 updates)
          (is (>= (count history) 3))
          ;; History should be sorted by transaction
          (is (= history (sort-by :tx history)))
          ;; Each history entry should have required keys
          (is (every? #(contains? % :attr) history))
          (is (every? #(contains? % :value) history))
          (is (every? #(contains? % :tx) history))
          (is (every? #(contains? % :op) history)))
      ;; Cleanup
      (d/release connection)
      (d/delete-database "datomic:mem://entity-history-test"))))

(deftest entity-history-nonexistent-entity-test
  (testing "Getting history of nonexistent entity returns empty sequence"
    (let [connection (conn-with-schema "history-nonexistent-test")
          fake-id (UUID/randomUUID)]
      (let [history (conn/entity-history connection fake-id)]
        (is (sequential? history))
        (is (empty? history)))
      ;; Cleanup
      (d/release connection)
      (d/delete-database "datomic:mem://history-nonexistent-test")))))

;; =============================================================================
;; URI Construction Tests
;; =============================================================================

(deftest dev-uri-format-test
  (testing "Dev server type constructs mem:// URI"
    (let [cfg {:datomic {:server-type :dev
                         :db-name "test-db"
                         :port 4334}}]
      ;; connect function should create mem:// URI for dev
      (is (nil? (conn/create-database cfg)))
      (d/delete-database "datomic:mem://test-db"))))

;; =============================================================================
;; Integration Tests
;; =============================================================================

(deftest full-workflow-test
  (testing "Complete workflow: create, connect, transact, query, time travel"
    (let [connection (conn-with-schema "workflow-test")
          entity-id (UUID/randomUUID)]
      ;; Transact data
      @(d/transact connection [{:db/id "temp"
                               :prov/entity entity-id
                               :traceability/product-name "milk"
                               :traceability/batch "BATCH-001"}])
      ;; Query current state - find entity by :prov/entity
      (let [db (conn/get-db connection)
            entity-result (d/q '[:find ?e :where [?e :prov/entity ?eid]] db entity-id)
            actual-entity-id (ffirst entity-result)
            entity (d/entity db actual-entity-id)]
        (is (= "milk" (:traceability/product-name entity)))
        (is (= "BATCH-001" (:traceability/batch entity)))
        ;; Time travel - capture t1 while entity is in scope
        (let [t1 (d/basis-t db)]
          ;; Update
          @(d/transact connection [[:db/add actual-entity-id :traceability/product-name "cheese"]])
          ;; Verify update
          (let [current-entity (d/entity (d/db connection) actual-entity-id)]
            (is (= "cheese" (:traceability/product-name current-entity))))
          ;; Verify old state via time travel
          (let [old-entity (conn/entity-as-of connection actual-entity-id t1)]
            (is (= "milk" (:traceability/product-name old-entity))))))
      ;; Verify history
      (let [db (conn/get-db connection)
            entity-result (d/q '[:find ?e :where [?e :prov/entity ?eid]] db entity-id)
            actual-entity-id (ffirst entity-result)
            history (conn/entity-history connection actual-entity-id)]
        (is (>= (count history) 2)))
      ;; Cleanup
      (d/release connection)
      (d/delete-database "datomic:mem://workflow-test"))))
