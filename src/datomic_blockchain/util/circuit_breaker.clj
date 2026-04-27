(ns datomic-blockchain.util.circuit-breaker
  "Circuit breaker pattern implementation for resilient cluster communication
   
   The circuit breaker prevents cascading failures by stopping requests to
   a failing service and allowing it time to recover.
   
   States:
   - CLOSED: Normal operation, requests pass through
   - OPEN: Failure threshold reached, requests fail fast
   - HALF-OPEN: Testing if service has recovered
   
   Usage:
     (require '[datomic-blockchain.util.circuit-breaker :as cb])
     
     ;; Create a circuit breaker
     (def node-breaker (cb/create {:failure-threshold 5
                                   :reset-timeout-ms 30000}))
     
     ;; Use it to protect calls
     (cb/call node-breaker
              #(send-message-to-node node message))
   "
  (:require [taoensso.timbre :as log])
  (:import [java.util.concurrent.atomic AtomicInteger]))

;; ============================================================================
;; Circuit Breaker States
;; ============================================================================

(def ^:private states
  {:closed "CLOSED"      ; Normal operation
   :open "OPEN"          ; Failing, reject requests
   :half-open "HALF-OPEN"}) ; Testing recovery

;; ============================================================================
;; Circuit Breaker Record
;; ============================================================================

(defrecord CircuitBreaker
  [name
   state                          ; Current state (:closed, :open, :half-open)
   failure-threshold              ; Failures before opening
   success-threshold              ; Successes in half-open to close
   reset-timeout-ms               ; Time before attempting reset
   failure-count                  ; Atomic counter for failures
   success-count                  ; Atomic counter for successes (in half-open)
   last-failure-time              ; Timestamp of last failure
   last-state-change              ; Timestamp of last state transition
   on-state-change                ; Callback for state changes
   on-open                        ; Callback when opened
   on-close])                     ; Callback when closed

;; ============================================================================
;; State Management
;; ============================================================================

(defn- transition-state!
  "Transition circuit breaker to a new state."
  [breaker new-state]
  (let [old-state @(:state breaker)]
    (when (not= old-state new-state)
      (reset! (:state breaker) new-state)
      (reset! (:last-state-change breaker) (System/currentTimeMillis))
      
      (log/info (str "Circuit breaker '" (:name breaker) "' transitioned: "
                     (name old-state) " -> " (name new-state)))
      
      ;; Reset counters
      (.set ^AtomicInteger (:failure-count breaker) 0)
      (.set ^AtomicInteger (:success-count breaker) 0)
      
      ;; Call callbacks
      (when-let [callback (:on-state-change breaker)]
        (callback old-state new-state))
      
      (case new-state
        :open (when-let [callback (:on-open breaker)] (callback))
        :closed (when-let [callback (:on-close breaker)] (callback))
        nil))))

(defn- should-attempt-reset?
  "Check if enough time has passed to try reset."
  [breaker]
  (let [last-failure @(:last-failure-time breaker)
        timeout (:reset-timeout-ms breaker)]
    (and last-failure
         (> (- (System/currentTimeMillis) last-failure) timeout))))

;; ============================================================================
;; Result Recording
;; ============================================================================

(defn- record-success!
  "Record a successful call."
  [breaker]
  (case @(:state breaker)
    :half-open
    (let [successes (.incrementAndGet ^AtomicInteger (:success-count breaker))]
      (when (>= successes (:success-threshold breaker))
        (transition-state! breaker :closed)))
    
    :closed
    ;; Reset failure count on success
    (.set ^AtomicInteger (:failure-count breaker) 0)
    
    ;; In open state, don't record (shouldn't happen due to check)
    nil)
  
  :success)

(defn- record-failure!
  "Record a failed call."
  [breaker]
  (reset! (:last-failure-time breaker) (System/currentTimeMillis))
  
  (case @(:state breaker)
    :half-open
    (transition-state! breaker :open)
    
    :closed
    (let [failures (.incrementAndGet ^AtomicInteger (:failure-count breaker))]
      (when (>= failures (:failure-threshold breaker))
        (transition-state! breaker :open)))
    
    ;; In open state, don't record (shouldn't happen due to check)
    nil)
  
  :failure)

;; ============================================================================
;; Circuit Breaker API
;; ============================================================================

(defn create
  "Create a new circuit breaker.
   
   Options:
     :name               - Circuit breaker name (default: 'circuit-breaker')
     :failure-threshold  - Failures before opening (default: 5)
     :success-threshold  - Successes in half-open to close (default: 2)
     :reset-timeout-ms   - Time before reset attempt (default: 30000)
     :on-state-change    - Callback fn [old-state new-state]
     :on-open            - Callback fn []
     :on-close           - Callback fn []
   
   Example:
     (create {:name \"node-1\"
              :failure-threshold 3
              :reset-timeout-ms 10000})"
  [opts]
  (map->CircuitBreaker
   {:name (or (:name opts) "circuit-breaker")
    :state (atom :closed)
    :failure-threshold (or (:failure-threshold opts) 5)
    :success-threshold (or (:success-threshold opts) 2)
    :reset-timeout-ms (or (:reset-timeout-ms opts) 30000)
    :failure-count (AtomicInteger. 0)
    :success-count (AtomicInteger. 0)
    :last-failure-time (atom nil)
    :last-state-change (atom (System/currentTimeMillis))
    :on-state-change (:on-state-change opts)
    :on-open (:on-open opts)
    :on-close (:on-close opts)}))

(defn state
  "Get current state of circuit breaker."
  [breaker]
  @(:state breaker))

(defn closed?
  "Check if circuit breaker is closed (allowing requests)."
  [breaker]
  (= :closed (state breaker)))

(defn open?
  "Check if circuit breaker is open (rejecting requests)."
  [breaker]
  (= :open (state breaker)))

(defn half-open?
  "Check if circuit breaker is half-open (testing)."
  [breaker]
  (= :half-open (state breaker)))

(defn allow-request?
  "Check if a request should be allowed through."
  [breaker]
  (case (state breaker)
    :closed true
    :half-open true  ; Allow limited requests for testing
    :open (if (should-attempt-reset? breaker)
            (do (transition-state! breaker :half-open)
                true)
            false)))

(defn call
  "Execute a function with circuit breaker protection.
   
   If the circuit is open, throws an exception immediately.
   Otherwise, executes the function and records the result.
   
   Example:
     (call breaker #(http/get url))"
  [breaker f]
  (if (allow-request? breaker)
    (try
      (let [result (f)]
        (record-success! breaker)
        result)
      (catch Exception e
        (record-failure! breaker)
        (throw e)))
    (throw (ex-info (str "Circuit breaker '" (:name breaker) "' is OPEN")
                    {:circuit-breaker (:name breaker)
                     :state :open
                     :error :circuit-open}))))

(defn call-with-fallback
  "Execute a function with circuit breaker and fallback.
   
   If the circuit is open or the function fails, returns fallback value.
   
   Example:
     (call-with-fallback breaker
                         #(http/get url)
                         {:cached-data true})"
  [breaker f fallback]
  (if (allow-request? breaker)
    (try
      (let [result (f)]
        (record-success! breaker)
        result)
      (catch Exception e
        (record-failure! breaker)
        (log/warn "Circuit breaker call failed, using fallback:" (.getMessage e))
        fallback))
    (do
      (log/debug "Circuit breaker open, using fallback")
      fallback)))

;; ============================================================================
;; Statistics
;; ============================================================================

(defn stats
  "Get current statistics for circuit breaker."
  [breaker]
  {:name (:name breaker)
   :state (name (state breaker))
   :failure-count (.get ^AtomicInteger (:failure-count breaker))
   :success-count (.get ^AtomicInteger (:success-count breaker))
   :failure-threshold (:failure-threshold breaker)
   :success-threshold (:success-threshold breaker)
   :last-failure-time @(:last-failure-time breaker)
   :last-state-change @(:last-state-change breaker)})

(defn reset-breaker!
  "Manually reset circuit breaker to closed state."
  [breaker]
  (transition-state! breaker :closed)
  (log/info (str "Circuit breaker '" (:name breaker) "' manually reset")))

;; ============================================================================
;; Circuit Breaker Registry
;; ============================================================================

(defonce ^:private registry (atom {}))

(defn register
  "Register a circuit breaker in the global registry."
  [name breaker]
  (swap! registry assoc name breaker)
  breaker)

(defn get-breaker
  "Get a circuit breaker from the registry."
  [name]
  (get @registry name))

(defn get-or-create
  "Get existing breaker or create and register new one."
  [name opts]
  (or (get-breaker name)
      (register name (create (assoc opts :name name)))))

(defn all-stats
  "Get stats for all registered circuit breakers."
  []
  (into {} (map (fn [[name breaker]] [name (stats breaker)]) @registry)))

;; ============================================================================
;; HTTP Client Wrapper
;; ============================================================================

(defn wrap-http-client
  "Wrap an HTTP client function with circuit breaker.
   
   Usage:
     (def safe-http-get
       (wrap-http-client http/get {:name \"http-get\"
                                   :failure-threshold 3}))"
  [http-fn opts]
  (let [breaker (create opts)]
    (fn [& args]
      (call breaker #(apply http-fn args)))))

(comment
  ;; Example usage
  (def my-breaker (create {:name "test-breaker"
                           :failure-threshold 3
                           :reset-timeout-ms 5000}))
  
  ;; Normal usage
  (call my-breaker #(+ 1 2))
  
  ;; With fallback
  (call-with-fallback my-breaker
                      #(throw (Exception. "Failed"))
                      :default-value)
  
  ;; Check state
  (state my-breaker)
  (stats my-breaker)
  
  ;; Manual reset
  (reset-breaker! my-breaker)
  
  ;; Registry
  (register :node-1 (create {:name "node-1"}))
  (get-breaker :node-1)
  (all-stats)
  )
