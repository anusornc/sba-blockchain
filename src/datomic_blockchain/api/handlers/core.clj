(ns datomic-blockchain.api.handlers.core
  "Main handlers namespace - re-exports all handler functions
   Provides backward compatibility with the original handlers.clj"
  (:require
   ;; Import all handler namespaces
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
   ;; Original dependencies for backward compatibility
   [taoensso.timbre :as log]
   [datomic.api :as d]
   [datomic-blockchain.api.middleware :as middleware]
   [datomic-blockchain.query.sparql :as sparql]
   [datomic-blockchain.query.graph :as graph-query]
   [datomic-blockchain.ontology.kb :as kb]
   [datomic-blockchain.ontology.loader :as loader]
   [datomic-blockchain.permission.policy :as policy]
   [datomic-blockchain.crypto.ed25519 :as crypto]
   [datomic-blockchain.data.dataset-loader :as dataset]
   [datomic-blockchain.consensus.cluster :as cluster-consensus]
   [datomic-blockchain.cluster.member :as member])
  (:import [java.util UUID Date]))

;; ============================================================================
;; Re-export Common Functions
;; ============================================================================

(def valid-uuid? common/valid-uuid?)
(def parse-uuid-safe common/parse-uuid-safe)
(def validate-uuid-param common/validate-uuid-param)
(def validate-positive-int common/validate-positive-int)
(def sanitize-string-param common/sanitize-string-param)
(def parse-pagination common/parse-pagination)
(def paginated-response common/paginated-response)
(def get-val common/get-val)
(def extract-body-params common/extract-body-params)
(def keywordize-body-params common/keywordize-body-params)
(def success common/success)
(def error common/error)
(def not-found common/not-found)
(def validation-error common/validation-error)
;; Note: with-error-handling is a macro, use directly from common namespace

;; ============================================================================
;; Re-export Handler Functions
;; ============================================================================

(def ^:dynamic *connection* nil)
(def ^:dynamic *policy-store* nil)
(def ^:dynamic *config* nil)

;; Graph handlers
(defn handle-get-entity
  ([request] (graph/handle-get-entity request *connection*))
  ([request connection] (graph/handle-get-entity request connection)))

(defn handle-get-graph
  ([request] (graph/handle-get-graph request *connection*))
  ([request connection] (graph/handle-get-graph request connection)))

(defn handle-find-path
  ([request] (graph/handle-find-path request *connection*))
  ([request connection] (graph/handle-find-path request connection)))

(defn handle-get-stats
  ([request] (graph/handle-get-stats request *connection*))
  ([request connection] (graph/handle-get-stats request connection)))

;; Traceability handlers
(defn handle-trace-product
  ([request] (traceability/handle-trace-product request *connection*))
  ([request connection] (traceability/handle-trace-product request connection)))

(defn handle-get-provenance
  ([request] (traceability/handle-get-provenance request *connection*))
  ([request connection] (traceability/handle-get-provenance request connection)))

(defn handle-get-timeline
  ([request] (traceability/handle-get-timeline request *connection*))
  ([request connection] (traceability/handle-get-timeline request connection)))

(defn handle-trace-by-qr
  ([request] (traceability/handle-trace-by-qr request *connection*))
  ([request connection] (traceability/handle-trace-by-qr request connection)))

;; Query handlers
(defn handle-query
  ([request] (query/handle-query request *connection*))
  ([request connection] (query/handle-query request connection)))

(def query-allowed? query/query-allowed?)

;; Block handlers
(defn handle-list-blocks
  ([request] (blocks/handle-list-blocks request *connection*))
  ([request connection] (blocks/handle-list-blocks request connection)))

(defn handle-get-block
  ([request] (blocks/handle-get-block request *connection*))
  ([request connection] (blocks/handle-get-block request connection)))

;; Ontology handlers
(defn handle-list-ontologies
  ([request] (ontology/handle-list-ontologies request *connection*))
  ([request connection] (ontology/handle-list-ontologies request connection)))

(defn handle-get-ontology
  ([request] (ontology/handle-get-ontology request *connection*))
  ([request connection] (ontology/handle-get-ontology request connection)))

;; Permission handlers
(defn handle-check-permission
  ([request] (permission/handle-check-permission request *connection* *policy-store*))
  ([request connection policy-store]
   (permission/handle-check-permission request connection policy-store)))

;; Transaction handlers
(defn handle-submit-transaction
  ([request] (transactions/handle-submit-transaction request *connection*))
  ([request connection] (transactions/handle-submit-transaction request connection)))

(defn handle-transaction-status
  ([request] (transactions/handle-transaction-status request *connection*))
  ([request connection] (transactions/handle-transaction-status request connection)))

(defn handle-list-pending-transactions
  ([request] (transactions/handle-list-pending-transactions request *connection*))
  ([request connection] (transactions/handle-list-pending-transactions request connection)))

;; Cluster handlers
(defn handle-internal-propose
  ([request] (cluster/handle-internal-propose request *connection*))
  ([request connection] (cluster/handle-internal-propose request connection)))

(defn handle-internal-vote
  ([request] (cluster/handle-internal-vote request *connection*))
  ([request connection] (cluster/handle-internal-vote request connection)))

(defn handle-internal-commit
  ([request] (cluster/handle-internal-commit request *connection*))
  ([request connection] (cluster/handle-internal-commit request connection)))

(defn handle-internal-rollback
  ([request] (cluster/handle-internal-rollback request *connection*))
  ([request connection] (cluster/handle-internal-rollback request connection)))

(defn handle-internal-cluster-status
  ([request] (cluster/handle-internal-cluster-status request *connection*))
  ([request connection] (cluster/handle-internal-cluster-status request connection)))

(def verify-node-auth cluster/verify-node-auth)

;; Dev handlers
(defn handle-load-sample-data
  ([request] (dev/handle-load-sample-data request *connection*))
  ([request connection] (dev/handle-load-sample-data request connection)))

(defn handle-create-test-blocks
  ([request] (dev/handle-create-test-blocks request *connection*))
  ([request connection] (dev/handle-create-test-blocks request connection)))

;; ============================================================================
;; Legacy Handler (Backward Compatibility)
;; ============================================================================

;; Dynamic vars are defined above to support wrapper functions in this namespace.

(defmacro with-connection
  "Execute body with Datomic connection bound (legacy)"
  [conn & body]
  `(binding [*connection* ~conn]
     ~@body))

;; ============================================================================
;; Legacy Health Check
;; ============================================================================

(defn handle-health
  "Health check endpoint"
  [request]
  (log/info "Health check")
  (middleware/success-response
   {:status :healthy
    :timestamp (Date.)
    :version "1.1.0-Refactored"
    :phase "Handlers Extracted into Modules"}))

;; ============================================================================
;; Legacy Error Handlers
;; ============================================================================

(defn handle-not-found
  "Handle 404 errors"
  [request]
  (middleware/error-response "Endpoint not found" 404))

(defn handle-method-not-allowed
  "Handle 405 errors"
  [request]
  (middleware/error-response "Method not allowed" 405))

;; ============================================================================
;; Migration Notice
;; ============================================================================

(comment
  "
  Handler Refactoring Complete!
  
  The monolithic handlers.clj has been split into focused modules:
  
  1. handlers/common.clj    - Shared utilities and validation
  2. handlers/graph.clj     - Graph visualization handlers
  3. handlers/traceability.clj - Supply chain traceability
  4. handlers/query.clj     - Safe Datomic query execution
  5. handlers/blocks.clj    - Block explorer
  6. handlers/ontology.clj  - Ontology management
  7. handlers/permission.clj - Access control
  8. handlers/transactions.clj - Transaction submission
  9. handlers/cluster.clj   - Internal cluster consensus
  10. handlers/dev.clj      - Development helpers
  11. handlers/core.clj     - This file - main re-export namespace
  
  All functions are re-exported here for backward compatibility.
  New code should require specific handler namespaces directly.
  
  Before: (require '[datomic-blockchain.api.handlers :as handlers])
  After:  (require '[datomic-blockchain.api.handlers.graph :as graph])
  
  Benefits:
  - Smaller, focused namespaces
  - Better testability
  - Clearer dependencies
  - Easier navigation
  
  Lines of code reduction:
  - Original handlers.clj: ~1540 lines
  - New modules combined: ~1200 lines
  - Savings: ~340 lines (22%) through deduplication
  
  To update routes.clj:
  1. Change require from handlers to handlers.core
  2. Or use specific handler namespaces
  
  Example migration in routes:
  
  ;; Old
  (:require [datomic-blockchain.api.handlers :as handlers])
  (GET \"/api/graph/entity/:id\" [id :as request]
    (handlers/handle-get-entity request))
  
  ;; New
  (:require [datomic-blockchain.api.handlers.graph :as graph-handlers])
  (GET \"/api/graph/entity/:id\" [id :as request]
    (graph-handlers/handle-get-entity request *connection*))
  
  Note: New handler functions accept connection as explicit parameter
  instead of relying on dynamic binding for better testability.
  
  TODO for full migration:
  1. Update routes.clj to use specific handler namespaces
  2. Remove dynamic binding usage
  3. Pass dependencies explicitly
  4. Delete original handlers.clj
  
  Current status: BACKWARD COMPATIBLE
  All existing code continues to work via this re-export namespace.
  
  Recommended path:
  1. Phase 1: Use handlers.core (current - backward compatible)
  2. Phase 2: Gradually migrate to specific namespaces
  3. Phase 3: Remove handlers.core, use specific namespaces directly
  
  "
  )
