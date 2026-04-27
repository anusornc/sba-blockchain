(ns datomic-blockchain.api.middleware
  "Middleware for REST API
  Provides logging, error handling, JSON response, and JWT authentication"
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [taoensso.timbre :as log]
            [buddy.sign.jwt :as jwt]
            [buddy.core.keys :as keys]
            [ring.middleware.params :as params]
            [ring.middleware.keyword-params :as keyword-params]
            [ring.middleware.json :as ring-json]
            [ring.util.response :as response]
            [clojure.walk :as walk])
  (:import [java.time Instant]))

;; ============================================================================
;; Configuration
;; ============================================================================

;; Production Mode
(def ^:dynamic *production-mode*
  "When true, enforces security best practices"
  (boolean (System/getenv "PRODUCTION_MODE")))

;; Minimum JWT secret length (characters) - increased to 64 for security
(def ^:private min-jwt-secret-length 64)

;; Get JWT Secret - SECURITY: Checked when actually used (lazy validation)
;; In test mode, returns a test secret if JWT_SECRET not set
(defn- get-jwt-secret
  "Get JWT secret from environment, validating it's properly configured.
   Throws exception if not set or too short (unless in test mode)."
  []
  (let [secret (System/getenv "JWT_SECRET")
        test-mode? (or (System/getProperty "kaocha.test.mode")
                       (Boolean/getBoolean "kaocha.test.mode")
                       (boolean (System/getenv "KAOCHA_TEST_MODE"))
                       (boolean (System/getenv "kaocha_test_mode")))]
    (cond
      ;; Test mode: use test secret if JWT_SECRET not set
      (and test-mode? (nil? secret))
      (do
        (log/debug "Test mode: using test JWT secret")
        "test-jwt-secret-for-unit-testing-purposes-only-min-64-chars-long")

      ;; No secret set - FAIL FAST in production
      (nil? secret)
      (throw (ex-info "JWT_SECRET environment variable must be set (required in production)"
                     {:error :missing-secret
                      :min-length min-jwt-secret-length
                      :usage "Set JWT_SECRET environment variable to at least 64 random characters"
                      :test-mode "Set KAOCHA_TEST_MODE=1 for unit tests"}))

      ;; Secret too short - FAIL FAST
      (< (count secret) min-jwt-secret-length)
      (throw (ex-info (str "JWT_SECRET must be at least " min-jwt-secret-length " characters for security")
                     {:error :secret-too-short
                      :provided-length (count secret)
                      :min-length min-jwt-secret-length
                      :usage "Generate a secure secret: openssl rand -base64 64"}))

      ;; Valid secret
      :else
      (do
        (log/debug "JWT_SECRET loaded successfully (length:" (count secret) "characters)")
        secret))))

;; Allowed CORS origins - SECURITY: In production, set via ALLOWED_ORIGINS env var
(def ^:private allowed-origins
  (let [origins-env (System/getenv "ALLOWED_ORIGINS")]
    (cond
      ;; Production mode requires explicit ALLOWED_ORIGINS
      (and *production-mode* (nil? origins-env))
      (throw (ex-info "ALLOWED_ORIGINS environment variable must be set in production mode"
                     {:error :missing-cors-config
                      :production-mode true}))

      ;; Parse origins from env var
      origins-env
      (set (str/split origins-env #","))

      ;; Dev mode defaults
      :else
      (do
        (log/warn "ALLOWED_ORIGINS not set - using permissive localhost origins (development mode)")
        #{"http://localhost:3000"
          "http://localhost:8080"
          "http://localhost:8280"
          "http://127.0.0.1:3000"
          "http://127.0.0.1:8280"
          "http://10.1.161.4:8280"
          "http://10.1.161.4:3000"}))))

;; ============================================================================
;; JSON Response Middleware
;; ============================================================================

(defn json-write-date
  "Custom date writer for JSON serialization"
  [date ^java.io.Writer writer]
  (.write writer (.toString (Instant/ofEpochMilli (.getTime date)))))

;; Configure JSON serialization
(def json-options
  {:date-fn json-write-date
   :key-fn name
   :value-fn (fn [k v]
               (if (instance? clojure.lang.Keyword v)
                 (name v)
                 v))})

(defn json-response
  "Convert response body to JSON"
  [body & [status]]
  (let [status (or status 200)]
    (-> body
        (json/write-str json-options)
        (response/response)
        (response/content-type "application/json")
        (response/status status))))

(defn wrap-json-response
  "Middleware to convert response maps to JSON"
  [handler]
  (fn [request]
    (let [response (handler request)]
      (if (map? response)
        (-> (:body response)
            (json-response (:status response 200))
            (assoc :headers (merge {"Content-Type" "application/json"}
                                  (:headers response {}))))
        response))))

;; ============================================================================
;; Logging Middleware
;; ============================================================================

(defn wrap-log-request
  "Log incoming requests (without sensitive data)"
  [handler]
  (fn [request]
    (log/info "API Request:"
              (:request-method request)
              (:uri request)
              ;; Don't log full params to avoid logging sensitive data
              (select-keys (:params request) [:page :per-page :id]))
    (handler request)))

(defn wrap-log-response
  "Log response status"
  [handler]
  (fn [request]
    (let [response (handler request)]
      (log/info "API Response:" (:status response))
      response)))

;; ============================================================================
;; Error Handling Middleware (Sanitized)
;; ============================================================================

(defn sanitize-error-message
  "Remove sensitive information from error messages before sending to clients"
  [^String msg]
  (cond
    ;; Don't reveal file paths
    (str/includes? msg ".java")
    "Internal server error"

    ;; Don't reveal database schema details
    (or (str/includes? msg ":db/")
        (str/includes? msg ":prov/")
        (str/includes? msg ":blockchain/"))
    "Data processing error"

    ;; Don't reveal class names
    (str/includes? msg "class ")
    "Internal server error"

    ;; Generic sanitization - remove stack trace patterns
    (re-find #"\ \[.*?\]" msg)
    "Internal server error"

    ;; Return safe messages as-is
    :else
    msg))

(defn wrap-exception
  "Catch exceptions and return sanitized error response"
  [handler]
  (fn [request]
    (try
      (handler request)
      (catch Exception e
        (log/error "API Error:" (.getMessage e) "\n" (str e))
        {:status 500
         :headers {"Content-Type" "application/json"}
         :body (json/write-str {:error (sanitize-error-message (.getMessage e))
                               :status :error
                               :timestamp (str (java.util.Date.))})}))))

(defn wrap-validation-error
  "Handle validation errors"
  [handler]
  (fn [request]
    (try
      (handler request)
      (catch clojure.lang.ExceptionInfo e
        (let [data (ex-data e)]
          (log/warn "Validation error:" data)
          {:status 400
           :headers {"Content-Type" "application/json"}
           :body (json/write-str {:error (or (:error data) "Validation failed")
                                 :status :validation-error
                                 :timestamp (str (java.util.Date.))})})))))

;; ============================================================================
;; CORS Middleware (Restricted)
;; ============================================================================

(defn wrap-cors
  "Add CORS headers to response
  Only allows configured origins to prevent CSRF attacks"
  [handler]
  (fn [request]
    (let [origin (get-in request [:headers "origin"])
            ;; Check if origin is allowed
            origin-allowed (and origin
                               (contains? allowed-origins origin))

            ;; Handle OPTIONS preflight request
            response (if (= :options (:request-method request))
                       {:status 200
                        :headers {"Access-Control-Allow-Origin"
                                  (if origin-allowed origin "")
                                  "Access-Control-Allow-Credentials" "true"
                                  "Access-Control-Allow-Methods" "GET, POST, PUT, DELETE, OPTIONS"
                                  "Access-Control-Allow-Headers" "Content-Type, Authorization"
                                  "Access-Control-Max-Age" "86400"}
                        :body ""}
                       (handler request))

            headers (:headers response {})]

      (assoc-in response [:headers]
                (merge {"Access-Control-Allow-Origin"
                        (if origin-allowed origin "")
                        "Access-Control-Allow-Credentials" "true"
                        "Access-Control-Allow-Methods" "GET, POST, PUT, DELETE, OPTIONS"
                        "Access-Control-Allow-Headers" "Content-Type, Authorization"
                        "Access-Control-Max-Age" "86400"}
                       (when origin-allowed
                         {"Access-Control-Allow-Origin" origin})
                       headers)))))

;; ============================================================================
;; JWT Authentication Middleware
;; ============================================================================

(defn generate-token
  "Generate a JWT token for a user

  Options:
    :sub - subject (user ID)
    :exp - expiration in seconds (default 3600 = 1 hour)
    :roles - vector of roles for authorization"
  ([user-id]
   (generate-token user-id nil))
  ([user-id opts]
   (let [now (System/currentTimeMillis)
         exp (+ now (* 1000 (or (:exp opts) 3600)))
         claims (merge {:sub (str user-id)
                       :iat now
                       :exp exp}
                      (select-keys opts [:roles]))
         secret (get-jwt-secret)]
     (jwt/sign claims secret {:alg :hs256}))))

(defn verify-token
  "Verify a JWT token and return claims if valid
  Returns nil if token is invalid or expired"
  [token]
  (try
    (jwt/unsign token (get-jwt-secret) {:alg :hs256})
    (catch Exception e
      (log/debug "Token verification failed:" (.getMessage e))
      nil)))

(defn extract-auth-token
  "Extract Bearer token from Authorization header"
  [headers]
  (when-let [auth (get headers "authorization")]
    (when (str/starts-with? auth "Bearer ")
      (subs auth 7))))

(defn valid-auth-token?
  "Validate auth token using JWT verification

  Returns claims map if valid, nil otherwise"
  [token]
  (when token
    (verify-token token)))

(defn wrap-authenticated
  "Require JWT authentication for endpoint

  Adds :user-claims to request on successful authentication"
  ([handler]
   (wrap-authenticated handler nil))
  ([handler opts]
   (fn [request]
     (log/info "Checking authentication for:" (:uri request))
     (if-let [auth-token (extract-auth-token (:headers request))]
       (if-let [claims (valid-auth-token? auth-token)]
         ;; Check if token is expired
         (let [now (System/currentTimeMillis)
               exp (:exp claims)]
           (if (or (nil? exp) (> exp now))
             ;; Token valid, add claims to request
             (do
               (log/info "Token valid, calling handler")
               (handler (assoc request :user-claims claims)))
             ;; Token expired
             (do
               (log/info "Token expired")
               {:status 401
                :headers {"Content-Type" "application/json"}
                :body (json/write-str {:error "Token expired"
                                      :status :unauthorized
                                      :timestamp (str (java.util.Date.))})})))
         ;; Token invalid
         (do
           (log/info "Token invalid")
           {:status 401
            :headers {"Content-Type" "application/json"}
            :body (json/write-str {:error "Invalid token"
                                  :status :unauthorized
                                  :timestamp (str (java.util.Date.))})}))
       ;; No token provided
       (do
         (log/info "No token provided, returning 401")
         {:status 401
          :headers {"Content-Type" "application/json"}
          :body (json/write-str {:error "Missing authorization header"
                                :status :unauthorized
                                :timestamp (str (java.util.Date.))})})))))

(defn wrap-optional-auth
  "Optional authentication - adds :user-claims if token present
  Does not block requests without token"
  [handler]
  (fn [request]
    (if-let [auth-token (extract-auth-token (:headers request))]
      (if-let [claims (valid-auth-token? auth-token)]
        (handler (assoc request :user-claims claims))
        (handler request))
      (handler request))))

(defn wrap-require-role
  "Require specific role for endpoint
  Use after wrap-authenticated"
  [handler required-role]
  (fn [request]
    (if-let [claims (:user-claims request)]
      (let [user-roles (set (:roles claims []))]
        (if (contains? user-roles required-role)
          (handler request)
          {:status 403
           :headers {"Content-Type" "application/json"}
           :body (json/write-str {:error "Insufficient permissions"
                                 :status :forbidden
                                 :timestamp (str (java.util.Date.))})}))
      ;; No authentication data
      {:status 401
       :headers {"Content-Type" "application/json"}
       :body (json/write-str {:error "Authentication required"
                             :status :unauthorized
                             :timestamp (str (java.util.Date.))})})))

;; ============================================================================
;; Content Negotiation Middleware
;; ============================================================================

(defn wrap-content-type
  "Handle content-type negotiation"
  [handler]
  (fn [request]
    (let [accept (get-in request [:headers "accept"] "application/json")]
      (handler (assoc request :accept accept)))))

;; ============================================================================
;; Request ID Middleware
;; ============================================================================

(defn wrap-request-id
  "Add unique request ID to each request"
  [handler]
  (fn [request]
    (let [request-id (str (java.util.UUID/randomUUID))]
      (log/with-context {:request-id request-id}
        (let [response (handler request)]
          (assoc-in response [:headers "X-Request-ID"] request-id))))))

;; ============================================================================
;; Parameter Parsing Middleware
;; ============================================================================

(defn wrap-params
  "Parse query and body parameters"
  [handler]
  (-> handler
      params/wrap-params
      keyword-params/wrap-keyword-params
      ring-json/wrap-json-response
      ring-json/wrap-json-body))

;; ============================================================================
;; API Response Helpers
;; ============================================================================

(defn success-response
  "Create successful API response"
  ([data]
   (success-response data 200))
  ([data status]
   {:status status
    :headers {"Content-Type" "application/json"}
    :body (json/write-str {:success true
                           :data data
                           :timestamp (str (java.util.Date.))})}))

(defn error-response
  "Create error API response"
  ([error-message]
   (error-response error-message 500))
  ([error-message status]
   {:status status
    :headers {"Content-Type" "application/json"}
    :body (json/write-str {:success false
                           :error error-message
                           :timestamp (str (java.util.Date.))})}))

(defn paginated-response
  "Create paginated API response"
  [data total page per-page]
  {:status 200
   :headers {"Content-Type" "application/json"}
   :body (json/write-str {:success true
                          :data data
                          :pagination {:total total
                                      :page page
                                      :per-page per-page
                                      :total-pages (Math/ceil (/ total per-page))}
                          :timestamp (str (java.util.Date.))})})

;; ============================================================================
;; Security Headers Middleware
;; ============================================================================

(def ^:private development-csp
  "CSP policy for development mode - allows localhost frontend"
  "default-src 'self'; script-src 'self' 'unsafe-eval' 'unsafe-inline'; style-src 'self' 'unsafe-inline' 'unsafe-hashes'; img-src 'self' data: https:; font-src 'self'; connect-src 'self' http://localhost:* http://127.0.0.1:* ws://localhost:* ws://127.0.0.1:*; frame-ancestors 'none'; base-uri 'self'; form-action 'self'; object-src 'none'; upgrade-insecure-requests")

(def ^:private production-csp
  "CSP policy for production mode - strict self-only"
  "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-hashes'; img-src 'self' data: https:; font-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'; object-src 'none'; upgrade-insecure-requests")

(defn wrap-security-headers
  "Add security-related headers to response"
  [handler]
  (fn [request]
    (let [response (handler request)
          csp (if *production-mode* production-csp development-csp)]
      (update-in response [:headers]
                 merge {"X-Content-Type-Options" "nosniff"
                        "X-Frame-Options" "DENY"
                        "X-XSS-Protection" "1; mode=block"
                        "Strict-Transport-Security" "max-age=31536000; includeSubDomains"
                        "Content-Security-Policy" csp
                        "Referrer-Policy" "strict-origin-when-cross-origin"
                        "Permissions-Policy" "geolocation=(), microphone=(), camera=(), payment=(), usb=(), magnetometer=(), gyroscope=()"}))))

;; ============================================================================
;; Development Mode Helpers
;; ============================================================================

(defn development-mode?
  "Check if the application is running in development mode.
   Returns true when PRODUCTION_MODE is NOT set."
  []
  (not *production-mode*))

(defn wrap-development-only
  "Middleware that only allows the handler in development mode.
   In production, returns 404 for all requests to these routes."
  [handler]
  (fn [request]
    (if (development-mode?)
      (handler request)
      {:status 404
       :headers {"Content-Type" "application/json"}
       :body (json/write-str {:error "Endpoint not found"
                             :status :not-found
                             :timestamp (str (java.util.Date.))})})))

;; ============================================================================
;; Development Helpers
;; ============================================================================

(comment
  ;; Generate token for testing
  (generate-token #uuid "550e8400-e29b-41d4-a716-446655440000")

  ;; Generate token with roles and expiration
  (generate-token "user-id" {:roles [:admin :user] :exp 7200})

  ;; Verify token
  (verify-token "eyJhbGciOiJIUzI1NiJ9...")

  ;; Create test request
  (def test-request
    {:request-method :get
     :uri "/api/test"
     :headers {"authorization" "Bearer YOUR_TOKEN_HERE"}
     :params {}})

  ;; Usage example
  (defn handler
    "Sample handler using middleware"
    [request]
    (success-response {:message "Hello from API"
                      :user (:user-claims request)}))

  ;; Apply middleware
  (def app
    (-> handler
        wrap-log-request
        wrap-log-response
        wrap-exception
        wrap-cors
        wrap-security-headers
        wrap-request-id
        wrap-params))

  ;; Test request
  (app {:request-method :get
        :uri "/api/test"
        :headers {"authorization" "Bearer dev-token"}})
  )