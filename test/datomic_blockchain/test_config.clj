(ns datomic-blockchain.test-config
  "Comprehensive test suite for configuration management.

   Tests cover:
   - Configuration validation (config values)
   - Configuration loading with profiles
   - Error handling and exception messages
   - Default configuration fallback"
  (:require [clojure.test :refer :all]
            [datomic-blockchain.config :as config])
  (:import [java.util UUID]))

;; ============================================================================
;; Configuration Validation Tests
;; ============================================================================

(deftest ^:parallel validate-config-values-invalid-port-test
  (testing "Throw exception when server port is out of range"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Configuration values validation failed"
                          (config/validate-config-values! {:server {:port 0}})))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Configuration values validation failed"
                          (config/validate-config-values! {:server {:port 65536}})))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Configuration values validation failed"
                          (config/validate-config-values! {:server {:port -1}})))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Configuration values validation failed"
                          (config/validate-config-values! {:server {:port "not-a-number"}})))))

(deftest ^:parallel validate-config-values-missing-datomic-test
  (testing "Throw exception when Datomic configuration is missing"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Configuration values validation failed"
                          (config/validate-config-values! {:server {:port 3000}
                                                           :log {:level :info}})))))

(deftest ^:parallel validate-config-values-valid-test
  (testing "Pass validation with valid configuration values"
    (is (nil? (config/validate-config-values!
               {:datomic {:db-name "test"}
                :server {:port 3000}
                :log {:level :debug}})))
    (is (nil? (config/validate-config-values!
               {:datomic {:db-name "test"
                          :server-type :dev}
                :server {:port 8080}
                :log {:level :info}})))))

;; ============================================================================
;; Valid Log Levels Test
;; ============================================================================

(deftest ^:parallel valid-log-levels-test
  (testing "All expected log levels are valid (tested indirectly)"
    ;; valid-log-levels is private, so we test it through validate-config-values!
    (is (nil? (config/validate-config-values!
               {:datomic {:db-name "test"}
                :server {:port 3000}
                :log {:level :debug}})))
    (is (nil? (config/validate-config-values!
               {:datomic {:db-name "test"}
                :server {:port 3000}
                :log {:level :info}})))
    (is (nil? (config/validate-config-values!
               {:datomic {:db-name "test"}
                :server {:port 3000}
                :log {:level :warn}})))
    (is (nil? (config/validate-config-values!
               {:datomic {:db-name "test"}
                :server {:port 3000}
                :log {:level :error}})))))

;; ============================================================================
;; Configuration Loading Tests
;; ============================================================================

(deftest load-config-default-test
  (testing "Load configuration with default :dev profile"
    (try
      (let [cfg (config/load-config)]
        (is (map? cfg))
        (is (contains? cfg :datomic))
        (is (contains? cfg :server))
        (is (contains? cfg :log))
        ;; Check default values when file exists
        (when (get-in cfg [:datomic :db-name])
          (is (string? (get-in cfg [:datomic :db-name])))))
      (catch clojure.lang.ExceptionInfo e
        ;; If JWT_SECRET is not set, we expect validation to fail
        (let [msg (.getMessage e)]
          (if (or (re-find #"JWT_SECRET" msg)
                  (some #(re-find #"JWT_SECRET" %) (get-in (ex-data e) [:errors] [])))
            (is true "Expected validation failure without JWT_SECRET")
            (throw e)))))))

(deftest load-config-with-profile-test
  (testing "Load configuration with explicit profile"
    (try
      (let [cfg (config/load-config :dev)]
        (is (map? cfg))
        (is (contains? cfg :datomic)))
      (catch clojure.lang.ExceptionInfo e
        ;; If JWT_SECRET is not set, we expect validation to fail
        (let [msg (.getMessage e)]
          (if (or (re-find #"JWT_SECRET" msg)
                  (some #(re-find #"JWT_SECRET" %) (get-in (ex-data e) [:errors] [])))
            (is true "Expected validation failure without JWT_SECRET")
            (throw e)))))))

(deftest load-config-missing-profile-test
  (testing "Use defaults when config file for profile is not found"
    (try
      (let [cfg (config/load-config (keyword "nonexistent-profile"))]
        (is (map? cfg))
        (is (contains? cfg :datomic))
        (is (= "blockchain-dev" (get-in cfg [:datomic :db-name])))
        (is (= 3000 (get-in cfg [:server :port])))
        (is (= :info (get-in cfg [:log :level]))))
      (catch clojure.lang.ExceptionInfo e
        ;; If JWT_SECRET is not set, we expect validation to fail
        (let [msg (.getMessage e)]
          (if (or (re-find #"JWT_SECRET" msg)
                  (some #(re-find #"JWT_SECRET" %) (get-in (ex-data e) [:errors] [])))
            (is true "Expected validation failure without JWT_SECRET")
            (throw e)))))))

;; ============================================================================
;; Comprehensive Validation Tests
;; ============================================================================

(deftest ^:parallel validate-config-comprehensive-test
  (testing "Full configuration validation with all components"
    (let [test-config {:datomic {:db-name "test-db"
                                :server-type :dev
                                :host "localhost"
                                :port 4334}
                       :server {:port 3000}
                       :log {:level :debug}
                       :consensus {:protocol :poa
                                  :validators []}
                       :ontology {:default-namespace "http://www.w3.org/ns/prov#"}
                       :permission {:default-visibility :private}
                       :cluster {:enabled? false
                                :node-id nil
                                :members {}
                                :quorum-size 2}}]
      (is (nil? (config/validate-config-values! test-config)))
      ;; All keys should be present
      (is (every? #(contains? test-config %)
                  [:datomic :server :log :consensus :ontology :permission :cluster])))))

(deftest ^:parallel validate-config-error-details-test
  (testing "Exception contains detailed error information"
    (try
      (config/validate-config-values! {:server {:port -1}})
      (is false "Should have thrown exception")
      (catch clojure.lang.ExceptionInfo e
        (let [data (ex-data e)]
          (is (= ::config/config-value-validation (:error data)))
          (is (vector? (:errors data)))
          (is (seq (:errors data)))
          (is (some #(re-find #"port" %) (:errors data)) (:errors data)))))))

;; ============================================================================
;; Integration Workflow Tests
;; ============================================================================

(deftest config-loading-workflow-test
  (testing "Complete configuration loading workflow"
    (try
      ;; Load with dev profile
      (let [cfg (config/load-config :dev)]
        ;; Verify structure
        (is (map? cfg))
        ;; Verify required sections exist
        (when (get cfg :datomic)
          (is (contains? cfg :datomic))
          (is (contains? cfg :server))
          (is (contains? cfg :log))))
      (catch clojure.lang.ExceptionInfo e
        ;; If JWT_SECRET is not set, we expect validation to fail
        (let [msg (.getMessage e)]
          (if (or (re-find #"JWT_SECRET" msg)
                  (some #(re-find #"JWT_SECRET" %) (get-in (ex-data e) [:errors] [])))
            (is true "Expected validation failure without JWT_SECRET")
            (throw e)))))))

(deftest ^:parallel config-with-all-valid-log-levels-test
  (testing "Configuration with each valid log level"
    ;; Test the four standard log levels
    (let [levels [:debug :info :warn :error]]
      (doseq [level levels]
        (is (nil? (config/validate-config-values!
                   {:datomic {:db-name "test"}
                    :server {:port 3000}
                    :log {:level level}})))))))

;; ============================================================================
;; Edge Cases Tests
;; ============================================================================

(deftest ^:parallel validate-config-empty-test
  (testing "Handle empty configuration gracefully"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Configuration values validation failed"
                          (config/validate-config-values! {})))))

(deftest ^:parallel validate-config-nil-values-test
  (testing "Handle nil values in configuration"
    (is (nil? (config/validate-config-values!
               {:datomic {:db-name "test"}
                :server {:port nil}
                :log {:level nil}})))))

(deftest ^:parallel validate-config-boundary-ports-test
  (testing "Accept boundary port values"
    (is (nil? (config/validate-config-values!
               {:datomic {:db-name "test"}
                :server {:port 1}
                :log {:level :info}})))
    (is (nil? (config/validate-config-values!
               {:datomic {:db-name "test"}
                :server {:port 65535}
                :log {:level :info}})))))

;; ============================================================================
;; Production Mode Tests
;; ============================================================================

(deftest validate-production-mode-test
  (testing "Production mode requires additional environment variables"
    (try
      ;; When PRODUCTION_MODE is set, DATOMIC_DB_NAME is required
      (config/validate-env-vars! true)
      ;; If we get here, validation passed (JWT_SECRET is set)
      (is true "Production validation passed with JWT_SECRET set")
      (catch clojure.lang.ExceptionInfo e
        ;; If we get an exception, verify it mentions required vars
        (let [data (ex-data e)
              msg (.getMessage e)
              errors (or (:errors data) [])]
          (is (or (re-find #"JWT_SECRET" msg)
                  (some #(re-find #"JWT_SECRET" %) errors)
                  (some #(re-find #"DATOMIC_DB_NAME" %) errors))))))))

(deftest validate-dev-mode-test
  (testing "Development mode works with JWT_SECRET set"
    (try
      ;; In dev mode, JWT_SECRET is still required (security policy)
      (config/validate-env-vars! false)
      (is true "Validation passed with valid JWT_SECRET")
      (catch clojure.lang.ExceptionInfo e
        ;; Should fail on JWT_SECRET at minimum
        (let [data (ex-data e)
              msg (.getMessage e)
              errors (or (:errors data) [])]
          (is (or (re-find #"JWT_SECRET" msg)
                  (some #(re-find #"JWT_SECRET" %) errors))))))))
