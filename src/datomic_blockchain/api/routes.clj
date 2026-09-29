(ns datomic-blockchain.api.routes
  "REST API routes: every handler receives its dependencies explicitly
   through closures — no dynamic binding (ADR-0001)."
  (:require [compojure.core :refer [GET POST context routes]]
            [compojure.route :as route]
            [taoensso.timbre :as log]
                        [ring.adapter.jetty :as jetty]
            [datomic-blockchain.api.middleware :as middleware]
            [datomic-blockchain.api.handlers.graph :as graph]
            [datomic-blockchain.api.handlers.traceability :as traceability]
            [datomic-blockchain.api.handlers.query :as query]
            [datomic-blockchain.api.handlers.blocks :as blocks]
            [datomic-blockchain.api.handlers.ontology :as ontology]
            [datomic-blockchain.api.handlers.permission :as permission]
            [datomic-blockchain.api.handlers.transactions :as transactions]
            [datomic-blockchain.api.handlers.cluster :as cluster]
            [datomic-blockchain.api.handlers.dev :as dev]
            [datomic-blockchain.cluster.member :as member]))

;; ============================================================================
;; Configuration
;; ============================================================================

(def ^:private transaction-auth-required?
  "Whether transaction API endpoints require authentication.
   Can be controlled via TRANSACTION_API_AUTH_REQUIRED env var.
   Default: true (require auth for security).
   Set to 'false' to allow benchmark scripts without JWT."
  (not= "false" (System/getenv "TRANSACTION_API_AUTH_REQUIRED")))

;; ============================================================================
;; Health and Fallback Handlers
;; ============================================================================

(defn handle-health
  "Health check endpoint"
  [request]
  (log/info "Health check")
  (middleware/success-response
   {:status :healthy
    :timestamp (java.util.Date.)
    :version "1.1.0-Refactored"
    :phase "Handlers Extracted into Modules"}))

(defn handle-not-found
  "Handle 404 errors"
  [request]
  (middleware/error-response "Endpoint not found" 404))

(defn handle-method-not-allowed
  "Handle 405 errors"
  [request]
  (middleware/error-response "Method not allowed" 405))

;; ============================================================================
;; Helper Functions for Authentication
;; ============================================================================

(defn require-auth
  "Check authentication and call handler if valid, otherwise return error"
  [handler request]
  (let [auth-token (middleware/extract-auth-token (:headers request))
        claims (when auth-token (middleware/valid-auth-token? auth-token))
        now (System/currentTimeMillis)
        exp (:exp claims)]
    (cond
      ;; No token provided
      (nil? auth-token)
      (middleware/error-response "Missing authorization header" 401)

      ;; Token invalid
      (nil? claims)
      (middleware/error-response "Invalid token" 401)

      ;; Token expired
      (and exp (<= exp now))
      (middleware/error-response "Token expired" 401)

      ;; Token valid - call handler
      :else
      (handler (assoc request :user-claims claims)))))

(defn optional-auth
  "Add optional authentication to request"
  [handler request]
  (let [auth-token (middleware/extract-auth-token (:headers request))
        claims (when auth-token (middleware/valid-auth-token? auth-token))]
    (if claims
      (handler (assoc request :user-claims claims))
      (handler request))))

(defn conditional-auth
  "Apply authentication conditionally based on configuration.
   If transaction-auth-required? is true, acts like require-auth.
   Otherwise, acts like optional-auth (for benchmarking)."
  [handler request]
  (if transaction-auth-required?
    (require-auth handler request)
    (optional-auth handler request)))

(defn require-admin
  "Check authentication AND admin role, then call handler
   Returns 403 if user is not an admin"
  [handler request]
  (let [auth-token (middleware/extract-auth-token (:headers request))
        claims (when auth-token (middleware/valid-auth-token? auth-token))
        now (System/currentTimeMillis)
        exp (:exp claims)
        ;; JWT uses JSON keys, so check both string and keyword
        user-roles (set (or (:roles claims) (get claims "roles") []))]
    (cond
      ;; No token provided
      (nil? auth-token)
      (middleware/error-response "Missing authorization header" 401)

      ;; Token invalid
      (nil? claims)
      (middleware/error-response "Invalid token" 401)

      ;; Token expired
      (and exp (<= exp now))
      (middleware/error-response "Token expired" 401)

      ;; Not an admin - check both keyword and string roles
      (not (or (contains? user-roles :admin)
               (contains? user-roles "admin")))
      (middleware/error-response "Admin privileges required" 403)

      ;; Token valid and admin - call handler
      :else
      (handler (assoc request :user-claims claims)))))

;; ============================================================================
;; Route Tables (functions of the dependencies they close over)
;; ============================================================================

(defn public-routes
  [conn]
  (routes
   ;; Health check - always public
   (GET "/health" request
        (handle-health request))

   ;; Public traceability for QR code scanning (consumer-facing).
   ;; Keep the QR lookup on a distinct path so authenticated /api/trace/:id
   ;; cannot be shadowed by this public route.
   (GET "/api/trace/qr/:qr" [qr :as request]
        (traceability/handle-trace-by-qr request conn))

   ;; Frontend-oriented public trace contract for the React ontology viewer.
   (GET "/api/ui/trace/:code" [code :as request]
        (traceability/handle-ui-trace request conn))

   ;; Frontend-oriented ontology vocabulary for legends and filters.
   (GET "/api/ui/ontology" request
        (traceability/handle-ui-ontology request conn))))

(defn development-routes
  [conn]
  (routes
   ;; Dev endpoint to load sample data (for demo/testing)
   (POST "/api/dev/load-sample-data" request
         (dev/handle-load-sample-data request conn))

   ;; Dev endpoint to create test blockchain transactions (for Block Explorer testing)
   (POST "/api/dev/create-test-blocks" request
         (dev/handle-create-test-blocks request conn))))

(defn optional-auth-routes
  [conn]
  (routes
   ;; Statistics can be accessed without auth (with optional user context)
   (GET "/api/stats" request
        (optional-auth (fn [req] (graph/handle-get-stats req conn)) request))

   ;; Block Explorer - public for demo purposes
   (GET "/api/blocks" request
        (optional-auth (fn [req] (blocks/handle-list-blocks req conn)) request))

   (GET "/api/blocks/:id" [id :as request]
        (optional-auth (fn [req] (blocks/handle-get-block req conn)) request))

   ;; Knowledge Graph - public for demo purposes (was authenticated)
   (GET "/api/graph/:id" [id :as request]
        (optional-auth (fn [req] (graph/handle-get-graph req conn)) request))

   ;; Ontology listing - public metadata
   (GET "/api/ontologies" request
        (optional-auth (fn [req] (ontology/handle-list-ontologies req conn)) request))

   (GET "/api/ontologies/:id" [id :as request]
        (optional-auth (fn [req] (ontology/handle-get-ontology req conn)) request))))

(defn authenticated-routes
  [conn policy-store]
  (routes
   ;; Graph endpoints - require auth (entity detail and path finding)
   (GET "/api/graph/entity/:id" [id :as request]
        (require-auth (fn [req] (graph/handle-get-entity req conn)) request))

   (GET "/api/graph/path" request
        (require-auth (fn [req] (graph/handle-find-path req conn)) request))

   ;; Traceability endpoints - require auth
   (GET "/api/trace/:id" [id :as request]
        (require-auth (fn [req] (traceability/handle-trace-product req conn)) request))

   (GET "/api/provenance/:id" [id :as request]
        (require-auth (fn [req] (traceability/handle-get-provenance req conn)) request))

   (GET "/api/timeline/:id" [id :as request]
        (require-auth (fn [req] (traceability/handle-get-timeline req conn)) request))

   ;; Query endpoint - require auth (critical security)
   (POST "/api/query" request
        (require-auth (fn [req] (query/handle-query req conn)) request))

   ;; Transaction submission and status API (for cluster benchmarking)
   ;; Authentication requirement controlled by TRANSACTION_API_AUTH_REQUIRED env var
   (POST "/api/transactions/submit" request
        (conditional-auth (fn [req] (transactions/handle-submit-transaction req conn)) request))

   (GET "/api/transactions/:id/status" [id :as request]
        (conditional-auth (fn [req] (transactions/handle-transaction-status req conn)) request))

   (GET "/api/transactions/pending" request
        (conditional-auth (fn [req] (transactions/handle-list-pending-transactions req conn)) request))

   ;; Permission endpoints - require auth
   (GET "/api/permissions/check" request
        (require-auth (fn [req] (permission/handle-check-permission req conn policy-store)) request))))

;; ============================================================================
;; Internal Cluster Routes (Node-to-Node Authentication)
;; These routes use X-Node-ID header for authentication instead of JWT.
;; They implement the propose-vote-commit consensus protocol.
;; ============================================================================

(defn verify-node-auth
  "Node-to-node auth gate for internal routes: the single predicate
   (handlers.cluster) decides, this wrapper adds the standard envelope."
  [handler request]
  (cond
    (not (member/cluster-enabled?))
    (middleware/error-response "Cluster mode not enabled" 503)

    :else
    (if-let [node-id (cluster/verify-node-auth request)]
      (handler (assoc request :node-id node-id))
      (middleware/error-response
       (if (seq (get-in request [:headers "x-node-id"]))
         "Unauthorized node"
         "Missing node authentication header")
       401))))

(defn internal-routes
  [conn]
  (routes
   ;; PROPOSE - Leader sends transaction proposal to all members
   (POST "/api/internal/propose" request
         (verify-node-auth (fn [req] (cluster/handle-internal-propose req conn)) request))

   ;; VOTE - Members send votes back to leader
   (POST "/api/internal/vote" request
         (verify-node-auth (fn [req] (cluster/handle-internal-vote req conn)) request))

   ;; COMMIT - Leader broadcasts commit after quorum reached
   (POST "/api/internal/commit" request
         (verify-node-auth (fn [req] (cluster/handle-internal-commit req conn)) request))

   ;; ROLLBACK - Leader broadcasts rollback on rejection
   (POST "/api/internal/rollback" request
         (verify-node-auth (fn [req] (cluster/handle-internal-rollback req conn)) request))

   ;; Cluster status - for monitoring
   (GET "/api/internal/cluster/status" request
        (verify-node-auth (fn [req] (cluster/handle-internal-cluster-status req conn)) request))))

;; ============================================================================
;; Combined Routes
;; ============================================================================

(defn api-routes
  ([conn] (api-routes conn nil))
  ([conn policy-store]
   (routes
    ;; Public routes (no auth)
    (public-routes conn)

    ;; Development-only routes (wrapped with dev-only middleware)
    (-> (development-routes conn)
        middleware/wrap-development-only)

    ;; Optional auth routes (auth adds user context)
    (optional-auth-routes conn)

    ;; Authenticated routes (require valid JWT)
    (authenticated-routes conn policy-store)

    ;; Internal cluster routes (node-to-node auth)
    (internal-routes conn)

    ;; 404 handler - must be last
    (route/not-found
     (fn [request]
       (middleware/error-response "Endpoint not found" 404))))))

;; ============================================================================
;; Middleware Application
;; ============================================================================

(defn wrap-api-middleware
  "Apply all middleware to routes"
  [handler]
  (-> handler
      middleware/wrap-log-request
      middleware/wrap-log-response
      middleware/wrap-exception
      middleware/wrap-validation-error
      middleware/wrap-cors
      middleware/wrap-security-headers
      middleware/wrap-request-id
      middleware/wrap-content-type
      middleware/wrap-params))

;; ============================================================================
;; Handler Creation and Server Startup
;; ============================================================================

(defn create-handler
  "API handler with dependencies injected through closures"
  ([conn policy-store]
   (create-handler conn policy-store nil))
  ([conn policy-store _config]
   (wrap-api-middleware (api-routes conn policy-store))))

(defn start-server
  "Start HTTP server with API
  Returns a map with :handler (for testing) and :server (Jetty instance)"
  [conn policy-store config & [port]]
  (let [port (or port 3000)
        handler (create-handler conn policy-store config)]
    (log/info "Starting API server on port" port)
    ;; Actually start Jetty server
    (let [jetty-server (jetty/run-jetty handler {:port port
                                                  :join? false
                                                  :allow-null-path-info true})]
      (log/info "Jetty server started on port" port)
      {:handler handler
       :port port
       :started true
       :server jetty-server})))

;; ============================================================================
;; Authentication Helpers (for external use)
;; ============================================================================

(defn generate-auth-token
  "Generate a JWT token for authentication

  Parameters:
    user-id: User identifier (UUID or string)
    opts: Optional map with :roles and :exp (expiration in seconds)

  Returns:
    JWT token string

  Example:
    (generate-auth-token user-id {:roles [:admin] :exp 7200})"
  [user-id & [opts]]
  (middleware/generate-token user-id opts))
