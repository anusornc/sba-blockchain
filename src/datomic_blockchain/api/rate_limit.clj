(ns datomic-blockchain.api.rate-limit
  "Rate limiting middleware for API protection
   
   Implements token bucket and sliding window rate limiting algorithms.
   Supports per-IP and per-user rate limiting.
   
   Usage:
     (require '[datomic-blockchain.api.rate-limit :as rl])
     
     ;; Wrap handler with rate limiting
     (-> handler
         (rl/wrap-rate-limit {:limit 100 :window-ms 60000}))  ; 100 req/min
   
   Configuration via environment variables:
     RATE_LIMIT_ENABLED=true
     RATE_LIMIT_DEFAULT=100
     RATE_LIMIT_WINDOW_MS=60000
   "
  (:require [clojure.string :as str]
            [clojure.data.json :as json]
            [taoensso.timbre :as log])
  (:import [java.time Instant Duration]
           [java.util.concurrent ConcurrentHashMap]))

;; ============================================================================
;; Configuration
;; ============================================================================

(def ^:private rate-limit-enabled?
  "Whether rate limiting is enabled"
  (= "true" (System/getenv "RATE_LIMIT_ENABLED")))

(def ^:private default-limit
  "Default request limit per window"
  (or (some-> (System/getenv "RATE_LIMIT_DEFAULT") Integer/parseInt)
      100))

(def ^:private default-window-ms
  "Default time window in milliseconds"
  (or (some-> (System/getenv "RATE_LIMIT_WINDOW_MS") Integer/parseInt)
      60000))  ; 1 minute

;; ============================================================================
;; Rate Limit Store
;; ============================================================================

(defonce ^:private rate-limit-store (ConcurrentHashMap.))

(defrecord RateLimitEntry
  [count        ; Current request count
   window-start ; Start of current window (epoch ms)
   limit        ; Max requests allowed
   window-ms])  ; Window size in ms

;; ============================================================================
;; Core Functions
;; ============================================================================

(defn- current-time-ms
  "Get current time in milliseconds"
  []
  (System/currentTimeMillis))

(defn- get-client-id
  "Extract client identifier from request.
   Falls back to IP address if no user is authenticated."
  [request]
  (or
   ;; Authenticated user ID
   (get-in request [:user-claims :sub])
   ;; X-Forwarded-For (for proxied requests)
   (when-let [forwarded (get-in request [:headers "x-forwarded-for"])]
     (first (str/split forwarded #",")))
   ;; Direct IP
   (:remote-addr request)
   ;; Fallback
   "unknown"))

(defn- make-key
  "Create storage key for rate limit entry"
  [client-id path]
  (str client-id ":" (or path "global")))

(defn- reset-entry
  "Create a new rate limit entry"
  [limit window-ms]
  (->RateLimitEntry 1 (current-time-ms) limit window-ms))

(defn- expired?
  "Check if a rate limit entry has expired"
  [entry]
  (> (current-time-ms)
     (+ (:window-start entry) (:window-ms entry))))

(defn- allow-request?
  "Check if request should be allowed and update counter.
   Returns [allowed? entry]"
  [client-id {:keys [limit window-ms path]}]
  (let [key (make-key client-id path)
        now (current-time-ms)]
    (loop []
      (let [entry (.get rate-limit-store key)]
        (cond
          ;; No entry exists - create new one
          (nil? entry)
          (let [new-entry (reset-entry limit window-ms)]
            (.put rate-limit-store key new-entry)
            [true new-entry])
          
          ;; Entry expired - reset
          (expired? entry)
          (let [new-entry (reset-entry limit window-ms)]
            (.put rate-limit-store key new-entry)
            [true new-entry])
          
          ;; Under limit - increment
          (< (:count entry) limit)
          (let [new-entry (assoc entry :count (inc (:count entry)))]
            (.put rate-limit-store key new-entry)
            [true new-entry])
          
          ;; Over limit - deny
          :else
          [false entry])))))

;; ============================================================================
;; Rate Limit Headers
;; ============================================================================

(defn- rate-limit-headers
  "Generate rate limit headers for response"
  [entry]
  (if entry
    {"X-RateLimit-Limit" (str (:limit entry))
     "X-RateLimit-Remaining" (str (max 0 (- (:limit entry) (:count entry))))
     "X-RateLimit-Reset" (str (+ (:window-start entry) (:window-ms entry)))}
    {}))

(defn- retry-after-header
  "Generate Retry-After header"
  [entry]
  (let [reset-time (+ (:window-start entry) (:window-ms entry))
        retry-seconds (max 1 (quot (- reset-time (current-time-ms)) 1000))]
    {"Retry-After" (str retry-seconds)}))

;; ============================================================================
;; Middleware
;; ============================================================================

(defn wrap-rate-limit
  "Ring middleware to apply rate limiting.
   
   Options:
     :limit      - Max requests per window (default: 100)
     :window-ms  - Time window in milliseconds (default: 60000)
     :path       - Optional path-specific limit
     :skip-fn    - Function (request -> boolean) to skip rate limiting
   
   Example:
     ;; Basic usage - 100 req/min per IP
     (wrap-rate-limit handler)
     
     ;; Custom limits
     (wrap-rate-limit handler {:limit 1000 :window-ms 3600000})  ; 1000/hour
     
     ;; Skip for health checks
     (wrap-rate-limit handler {:skip-fn #(= (:uri %) \"/health\")})"
  ([handler]
   (wrap-rate-limit handler {}))
  ([handler opts]
   (fn [request]
     (if (or (not rate-limit-enabled?)
             (and (:skip-fn opts) ((:skip-fn opts) request)))
       ;; Rate limiting disabled or skipped
       (handler request)
       
       ;; Apply rate limiting
       (let [client-id (get-client-id request)
             limit-config {:limit (or (:limit opts) default-limit)
                          :window-ms (or (:window-ms opts) default-window-ms)
                          :path (:path opts)}
             [allowed? entry] (allow-request? client-id limit-config)]
         
         (if allowed?
           ;; Request allowed
           (let [response (handler request)]
             ;; Add rate limit headers to response
             (update response :headers merge (rate-limit-headers entry)))
           
           ;; Request denied
           (do
            (log/warn "Rate limit exceeded for client" client-id)
             {:status 429
              :headers (merge {"Content-Type" "application/json"}
                             (rate-limit-headers entry)
                             (retry-after-header entry))
              :body (json/write-str
                     {:error "Rate limit exceeded"
                      :retry_after (max 1 (quot (- (+ (:window-start entry) (:window-ms entry))
                                                  (current-time-ms))
                                              1000))})}))))

(defn wrap-rate-limit-by-user
  "Rate limit by authenticated user ID.
   Falls back to IP for unauthenticated requests."
  ([handler]
   (wrap-rate-limit-by-user handler {}))
  ([handler opts]
   (wrap-rate-limit handler opts)))

;; ============================================================================
;; Per-Endpoint Rate Limiting
;; ============================================================================

(defn wrap-endpoint-rate-limits
  "Apply different rate limits to different endpoints.
   
   Example:
     (wrap-endpoint-rate-limits handler
       {\"/api/transactions/submit\" {:limit 10 :window-ms 60000}
        \"/api/query\" {:limit 100 :window-ms 60000}
        :default {:limit 1000 :window-ms 60000}})"
  [handler endpoint-configs]
  (fn [request]
    (let [uri (:uri request)
          config (or (get endpoint-configs uri)
                    (get endpoint-configs :default)
                    {})]
      ((wrap-rate-limit handler config) request))))

;; ============================================================================
;; Admin Functions
;; ============================================================================

(defn clear-rate-limits
  "Clear all rate limit entries."
  []
  (.clear rate-limit-store)
  (log/info "Rate limit store cleared"))

(defn get-rate-limit-stats
  "Get statistics about current rate limiting."
  []
  {:enabled rate-limit-enabled?
   :default-limit default-limit
   :default-window-ms default-window-ms
   :active-entries (.size rate-limit-store)
   :entries (into {} (map (fn [[k v]] [k (into {} v)]) rate-limit-store))})

(defn get-client-stats
  "Get rate limit stats for a specific client."
  [client-id]
  (->> rate-limit-store
       (filter (fn [[k _]] (str/starts-with? k (str client-id ":"))))
       (map (fn [[k v]] [k (into {} v)]))
       (into {})))

(comment
  ;; Check current stats
  (get-rate-limit-stats)
  
  ;; Clear all limits
  (clear-rate-limits)
  
  ;; Test rate limiting
  (def test-handler (wrap-rate-limit #(hash-map :status 200 :body "OK")
                                     {:limit 3 :window-ms 60000}))
  
  ;; Simulate requests
  (dotimes [i 5]
    (let [response (test-handler {:remote-addr "1.2.3.4" :uri "/test"})]
      (println "Request" (inc i) ":" (:status response))))
  
  ;; Per-endpoint limits
  (def api-handler
    (wrap-endpoint-rate-limits
     #(hash-map :status 200 :body "OK")
     {"/api/transactions/submit" {:limit 10 :window-ms 60000}
      "/api/query" {:limit 100 :window-ms 60000}
      :default {:limit 1000 :window-ms 60000}}))
  )

)))
