(ns datomic-blockchain.api.routes-v2
  "Updated REST API routes with new features
   
   Changes from v1:
   - Uses new handler modules instead of monolithic handlers.clj
   - Adds /metrics endpoint for Prometheus
   - Adds /openapi.yaml endpoint for API documentation
   - Integrates rate limiting middleware
   - Integrates metrics middleware
   - Better error handling with user-friendly messages"
  (:require [compojure.core :refer [defroutes GET POST PUT DELETE context routes]]
            [compojure.route :as route]
            [ring.util.response :as response]
            [taoensso.timbre :as log]
            [clojure.java.io :as io]
            [datomic-blockchain.api.middleware :as middleware]
            [datomic-blockchain.api.rate-limit :as rate-limit]
            [datomic-blockchain.api.metrics :as metrics]
            [datomic-blockchain.api.handlers.common :as common]
            [datomic-blockchain.api.handlers.graph :as graph-handlers]
            [datomic-blockchain.api.handlers.traceability :as trace-handlers]
            [datomic-blockchain.api.handlers.query :as query-handlers]
            [datomic-blockchain.api.handlers.blocks :as block-handlers]
            [datomic-blockchain.api.handlers.ontology :as ontology-handlers]
            [datomic-blockchain.api.handlers.permission :as permission-handlers]
            [datomic-blockchain.api.handlers.transactions :as tx-handlers]
            [datomic-blockchain.api.handlers.cluster :as cluster-handlers]
            [datomic-blockchain.api.handlers.dev :as dev-handlers]))

;; ============================================================================
;; Dependency Injection
;; ============================================================================

(def ^:dynamic *connection* nil)
(def ^:dynamic *policy-store* nil)

(defn- with-deps
  "Helper to call handler with dependencies"
  [handler-fn & args]
  (apply handler-fn (concat args [*connection* *policy-store*])))

;; ============================================================================
;; Authentication Helpers
;; ============================================================================

(defn- error-response
  "Create error response map"
  [error status]
  {:status status
   :headers {"Content-Type" "application/json"}
   :body (common/json-write-str
          {:success false
           :error error
           :timestamp (str (java.util.Date.))})})

(defn- require-auth
  "Check authentication and call handler if valid"
  [handler request]
  (let [auth-token (middleware/extract-auth-token (:headers request))
        claims (when auth-token (middleware/valid-auth-token? auth-token))
        now (System/currentTimeMillis)
        exp (:exp claims)]
    (cond
      (nil? auth-token)
      (error-response "Missing authorization header" 401)
      
      (nil? claims)
      (error-response "Invalid token" 401)
      
      (and exp (<= exp now))
      (error-response "Token expired" 401)
      
      :else
      (handler (assoc request :user-claims claims)))))

(defn- optional-auth
  "Add optional authentication to request"
  [handler request]
  (let [auth-token (middleware/extract-auth-token (:headers request))
        claims (when auth-token (middleware/valid-auth-token? auth-token))]
    (if claims
      (handler (assoc request :user-claims claims))
      (handler request))))

;; ============================================================================
;; Routes
;; ============================================================================

(defroutes public-routes
  ;; Health check
  (GET "/health" request
    (common/success
     {:status :healthy
      :version "1.1.0-RoutesV2"
      :timestamp (java.util.Date.)
      :features ["OpenAPI" "Prometheus" "RateLimiting" "CircuitBreaker"]}))
  
  ;; OpenAPI documentation
  (GET "/openapi.yaml" []
    (if-let [resource (io/resource "openapi/api.yaml")]
      {:status 200
       :headers {"Content-Type" "text/yaml; charset=utf-8"}
       :body (slurp resource)}
      {:status 404
       :body "OpenAPI specification not found"}))
  
  ;; Prometheus metrics
  (GET "/metrics" request
    (metrics/handle-metrics request))
  
  ;; Public traceability
  (GET "/api/trace/:qr" [qr :as request]
    (trace-handlers/handle-trace-by-qr (assoc request :params {:qr qr}) *connection*))
  
  ;; Dev endpoints (disable in production)
  (POST "/api/dev/load-sample-data" request
    (dev-handlers/handle-load-sample-data request *connection*))
  
  (POST "/api/dev/create-test-blocks" request
    (dev-handlers/handle-create-test-blocks request *connection*)))

(defroutes optional-auth-routes
  ;; Statistics
  (GET "/api/stats" request
    (optional-auth graph-handlers/handle-get-stats request *connection*))
  
  ;; Block explorer (public for demo)
  (GET "/api/blocks" request
    (block-handlers/handle-list-blocks request *connection*))
  
  (GET "/api/blocks/:id" [id :as request]
    (block-handlers/handle-get-block (assoc-in request [:params :id] id) *connection*))
  
  ;; Graph (public for demo)
  (GET "/api/graph/:id" [id :as request]
    (graph-handlers/handle-get-graph (assoc-in request [:params :id] id) *connection*))
  
  ;; Ontologies
  (GET "/api/ontologies" request
    (ontology-handlers/handle-list-ontologies request *connection*))
  
  (GET "/api/ontologies/:id" [id :as request]
    (ontology-handlers/handle-get-ontology (assoc-in request [:params :id] id) *connection*)))

(defroutes authenticated-routes
  ;; Graph (authenticated)
  (GET "/api/graph/entity/:id" [id :as request]
    (require-auth
     #(graph-handlers/handle-get-entity (assoc-in % [:params :id] id) *connection*)
     request))
  
  (GET "/api/graph/path" request
    (require-auth
     #(graph-handlers/handle-find-path % *connection*)
     request))
  
  ;; Traceability
  (GET "/api/trace/:id" [id :as request]
    (require-auth
     #(trace-handlers/handle-trace-product (assoc-in % [:params :id] id) *connection*)
     request))
  
  (GET "/api/provenance/:id" [id :as request]
    (require-auth
     #(trace-handlers/handle-get-provenance (assoc-in % [:params :id] id) *connection*)
     request))
  
  (GET "/api/timeline/:id" [id :as request]
    (require-auth
     #(trace-handlers/handle-get-timeline (assoc-in % [:params :id] id) *connection*)
     request))
  
  ;; Query
  (POST "/api/query" request
    (require-auth
     #(query-handlers/handle-query % *connection*)
     request))
  
  ;; Transactions (cluster)
  (POST "/api/transactions/submit" request
    (require-auth
     #(tx-handlers/handle-submit-transaction % *connection*)
     request))
  
  (GET "/api/transactions/:id/status" [id :as request]
    (require-auth
     #(tx-handlers/handle-transaction-status (assoc-in % [:params :id] id) *connection*)
     request))
  
  (GET "/api/transactions/pending" request
    (require-auth
     #(tx-handlers/handle-list-pending-transactions % *connection*)
     request))
  
  ;; Permissions
  (GET "/api/permissions/check" request
    (require-auth
     #(permission-handlers/handle-check-permission % *connection* *policy-store*)
     request)))

(defroutes internal-routes
  ;; Cluster consensus internal API
  (POST "/api/internal/propose" request
    (cluster-handlers/handle-internal-propose request *connection*))
  
  (POST "/api/internal/vote" request
    (cluster-handlers/handle-internal-vote request *connection*))
  
  (POST "/api/internal/commit" request
    (cluster-handlers/handle-internal-commit request *connection*))
  
  (POST "/api/internal/rollback" request
    (cluster-handlers/handle-internal-rollback request *connection*))
  
  (GET "/api/internal/cluster/status" request
    (cluster-handlers/handle-internal-cluster-status request *connection*)))

(defroutes api-routes
  public-routes
  optional-auth-routes
  authenticated-routes
  internal-routes
  
  (route/not-found
   (fn [request]
     (error-response "Endpoint not found. See /openapi.yaml for API documentation." 404))))

;; ============================================================================
;; Middleware Stack
;; ============================================================================

(defn- wrap-user-friendly-errors
  "Convert technical errors to user-friendly messages"
  [handler]
  (fn [request]
    (try
      (let [response (handler request)]
        (if (= 500 (:status response))
          (-> response
              (assoc :body (common/json-write-str
                            {:success false
                             :error "An unexpected error occurred. Please try again later."
                             :error-code :internal-error
                             :timestamp (str (java.util.Date.))})))
          response))
      (catch Exception e
        (log/error "Unhandled exception:" (.getMessage e))
        {:status 500
         :headers {"Content-Type" "application/json"}
         :body (common/json-write-str
                {:success false
                 :error "An unexpected error occurred. Please try again later."
                 :error-code :internal-error
                 :timestamp (str (java.util.Date.))})}))))

(defn wrap-api-middleware
  "Apply all middleware to routes"
  [handler]
  (-> handler
      middleware/wrap-log-request
      middleware/wrap-log-response
      wrap-user-friendly-errors
      middleware/wrap-validation-error
      middleware/wrap-cors
      middleware/wrap-security-headers
      middleware/wrap-request-id
      middleware/wrap-content-type
      middleware/wrap-params
      ;; Add rate limiting
      (rate-limit/wrap-rate-limit {:limit 1000 :window-ms 60000
                                   :skip-fn #(= (:uri %) "/health")})
      ;; Add metrics collection
      (metrics/wrap-metrics :api)))

;; ============================================================================
;; App Creation
;; ============================================================================

(def app
  "Main API application with middleware"
  (wrap-api-middleware api-routes))

(defn create-handler
  "Create API handler with dependencies"
  [conn policy-store]
  (fn [request]
    (binding [*connection* conn
              *policy-store* policy-store]
      (app request))))

;; ============================================================================
;; Server Startup
;; ============================================================================

(defn start-server
  "Start HTTP server with API v2
   Returns a map with :handler (for testing) and :server (Jetty instance)"
  [conn policy-store & [port]]
  (let [port (or port 3000)
        handler (create-handler conn policy-store)]
    (log/info "Starting API v2 server on port" port)
    (let [jetty-server (ring.adapter.jetty/run-jetty
                        handler
                        {:port port
                         :join? false
                         :allow-null-path-info true})]
      (log/info "Jetty server started on port" port)
      {:handler handler
       :port port
       :started true
       :server jetty-server})))

;; ============================================================================
;; Migration Guide Comment
;; ============================================================================

(comment
  "
  API Routes V2 Migration Guide
  
  New Features:
  1. Modular handler organization (no more 1500+ line handlers.clj)
  2. OpenAPI specification at /openapi.yaml
  3. Prometheus metrics at /metrics
  4. Rate limiting (1000 req/min default, configurable)
  5. Circuit breaker support for cluster communication
  6. User-friendly error messages
  
  To migrate from routes.clj:
  
  1. Update core.clj to use routes-v2:
     
     ;; Old
     (:require [datomic-blockchain.api.routes :as api])
     
     ;; New
     (:require [datomic-blockchain.api.routes-v2 :as api])
  
  2. Both namespaces have the same start-server function signature,
     so the rest of the code should work without changes.
  
  3. New endpoints available:
     GET  /openapi.yaml    - OpenAPI specification
     GET  /metrics         - Prometheus metrics
  
  4. Environment variables for configuration:
     RATE_LIMIT_ENABLED=true
     RATE_LIMIT_DEFAULT=1000
     RATE_LIMIT_WINDOW_MS=60000
  
  5. All existing endpoints work the same way.
  
  Benefits:
  - Better separation of concerns
  - Self-documenting API via OpenAPI
  - Observable via Prometheus
  - Protected from abuse via rate limiting
  - Resilient via circuit breakers
  "
  )
