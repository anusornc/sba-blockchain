(ns datomic-blockchain.api.routes
  "REST API routes
  Defines all API endpoints and routes to handlers"
  (:require [compojure.core :refer [defroutes GET POST PUT DELETE context routes]]
            [compojure.route :as route]
            [ring.util.response :as response]
            [taoensso.timbre :as log]
            [clojure.data.json :as json]
            [ring.adapter.jetty :as jetty]
            [datomic-blockchain.api.middleware :as middleware]
            [datomic-blockchain.api.handlers.core :as handlers]
            [datomic-blockchain.cluster.member :as member]))

;; ============================================================================
;; Dynamic Dependencies
;; ============================================================================

(def ^:dynamic *connection* nil)
(def ^:dynamic *policy-store* nil)

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
;; Helper Functions for Authentication
;; ============================================================================

(defn error-response
  "Create an error response map"
  [error status]
  {:status status
   :headers {"Content-Type" "application/json"}
   :body (json/write-str
          {:error error
           :status (if (= status 401) :unauthorized :not-found)
           :timestamp (str (java.util.Date.))})})

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
      (error-response "Missing authorization header" 401)

      ;; Token invalid
      (nil? claims)
      (error-response "Invalid token" 401)

      ;; Token expired
      (and exp (<= exp now))
      (error-response "Token expired" 401)

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
      (error-response "Missing authorization header" 401)

      ;; Token invalid
      (nil? claims)
      (error-response "Invalid token" 401)

      ;; Token expired
      (and exp (<= exp now))
      (error-response "Token expired" 401)

      ;; Not an admin - check both keyword and string roles
      (not (or (contains? user-roles :admin)
               (contains? user-roles "admin")))
      (error-response "Admin privileges required" 403)

      ;; Token valid and admin - call handler
      :else
      (handler (assoc request :user-claims claims)))))

;; ============================================================================
;; Public Routes (No Authentication Required)
;; ============================================================================

(defroutes public-routes
  ;; Health check - always public
  (GET "/health" request
        (handlers/handle-health request))

  ;; Public traceability for QR code scanning (consumer-facing)
  (GET "/api/trace/:qr" [qr :as request]
        (handlers/handle-trace-by-qr request)))

;; ============================================================================
;; Development-Only Routes (Only available in non-production mode)
;; ============================================================================

(defroutes development-routes
  ;; Dev endpoint to load sample data (for demo/testing)
  (POST "/api/dev/load-sample-data" request
        (handlers/handle-load-sample-data request))

  ;; Dev endpoint to create test blockchain transactions (for Block Explorer testing)
  (POST "/api/dev/create-test-blocks" request
        (handlers/handle-create-test-blocks request)))

;; ============================================================================
;; Optional Auth Routes (Work with or without authentication)
;; ============================================================================

(defroutes optional-auth-routes
  ;; Statistics can be accessed without auth (with optional user context)
  (GET "/api/stats" request
        (optional-auth handlers/handle-get-stats request))

  ;; Block Explorer - public for demo purposes
  (GET "/api/blocks" request
        (optional-auth handlers/handle-list-blocks request))

  (GET "/api/blocks/:id" [id :as request]
        (optional-auth handlers/handle-get-block request))

  ;; Knowledge Graph - public for demo purposes (was authenticated)
  (GET "/api/graph/:id" [id :as request]
        (optional-auth handlers/handle-get-graph request))

  ;; Ontology listing - public metadata
  (GET "/api/ontologies" request
        (optional-auth handlers/handle-list-ontologies request))

  (GET "/api/ontologies/:id" [id :as request]
        (optional-auth handlers/handle-get-ontology request)))

;; ============================================================================
;; Authenticated Routes (JWT Required)
;; ============================================================================

(defroutes authenticated-routes
  ;; Graph endpoints - require auth (entity detail and path finding)
  (GET "/api/graph/entity/:id" [id :as request]
       (require-auth handlers/handle-get-entity request))

  (GET "/api/graph/path" request
       (require-auth handlers/handle-find-path request))

  ;; Traceability endpoints - require auth
  (GET "/api/trace/:id" [id :as request]
       (require-auth handlers/handle-trace-product request))

  (GET "/api/provenance/:id" [id :as request]
       (require-auth handlers/handle-get-provenance request))

  (GET "/api/timeline/:id" [id :as request]
       (require-auth handlers/handle-get-timeline request))

  ;; Query endpoint - require auth (critical security)
  (POST "/api/query" request
        (require-auth handlers/handle-query request))

  ;; Transaction submission and status API (for cluster benchmarking)
  ;; Authentication requirement controlled by TRANSACTION_API_AUTH_REQUIRED env var
  (POST "/api/transactions/submit" request
        (conditional-auth handlers/handle-submit-transaction request))

  (GET "/api/transactions/:id/status" request
       (conditional-auth handlers/handle-transaction-status request))

  (GET "/api/transactions/pending" request
       (conditional-auth handlers/handle-list-pending-transactions request))

  ;; Permission endpoints - require auth
  (GET "/api/permissions/check" request
       (require-auth handlers/handle-check-permission request)))

;; ============================================================================
;; Internal Cluster Routes (Node-to-Node Authentication)
;; These routes use X-Node-ID header for authentication instead of JWT.
;; They implement the propose-vote-commit consensus protocol.
;; ============================================================================

(defn verify-node-auth
  "Check node-to-node authentication via X-Node-ID header."
  [handler request]
  (let [node-id (get-in request [:headers "x-node-id"])
        cluster-enabled? (member/cluster-enabled?)
        cluster-member (when cluster-enabled? (member/get-cluster))
        known-node? (and cluster-member node-id
                         (contains? (member/members cluster-member) node-id))]
    (cond
      (not cluster-enabled?)
      (error-response "Cluster mode not enabled" 503)

      (not (and node-id (seq node-id)))
      (error-response "Missing node authentication header" 401)

      (not known-node?)
      (error-response "Unauthorized node" 401)

      :else
      (handler (assoc request :node-id node-id)))))

(defroutes internal-routes
  ;; PROPOSE - Leader sends transaction proposal to all members
  (POST "/api/internal/propose" request
        (verify-node-auth handlers/handle-internal-propose request))

  ;; VOTE - Members send votes back to leader
  (POST "/api/internal/vote" request
        (verify-node-auth handlers/handle-internal-vote request))

  ;; COMMIT - Leader broadcasts commit after quorum reached
  (POST "/api/internal/commit" request
        (verify-node-auth handlers/handle-internal-commit request))

  ;; ROLLBACK - Leader broadcasts rollback on rejection
  (POST "/api/internal/rollback" request
        (verify-node-auth handlers/handle-internal-rollback request))

  ;; Cluster status - for monitoring
  (GET "/api/internal/cluster/status" request
       (verify-node-auth handlers/handle-internal-cluster-status request)))

;; ============================================================================
;; Combined Routes
;; ============================================================================

(defroutes api-routes
  ;; Public routes (no auth)
  public-routes

  ;; Development-only routes (wrapped with dev-only middleware)
  (-> development-routes
      middleware/wrap-development-only)

  ;; Optional auth routes (auth adds user context)
  optional-auth-routes

  ;; Authenticated routes (require valid JWT)
  authenticated-routes

  ;; Internal cluster routes (node-to-node auth)
  internal-routes

  ;; 404 handler - must be last
  (route/not-found
   (fn [request]
     (error-response "Endpoint not found" 404))))

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
;; Main App
;; ============================================================================

(def app
  "Main API application with middleware"
  (wrap-api-middleware api-routes))

(def app-routes
  "Route handler (used by create-handler)"
  api-routes)

;; ============================================================================
;; Handler Creation
;; ============================================================================

(defn create-handler
  "Create API handler with dependencies"
  ([conn policy-store]
   (create-handler conn policy-store nil))
  ([conn policy-store config]
   (fn [request]
     (binding [handlers/*connection* conn
               handlers/*policy-store* policy-store
               handlers/*config* config]
       (app request)))))

;; ============================================================================
;; Server Startup
;; ============================================================================

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
