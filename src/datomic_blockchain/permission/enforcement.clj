(ns datomic-blockchain.permission.enforcement
  "Access control enforcement layer
  Integrates permission checking with Datomic queries"
  (:require [clojure.spec.alpha :as s]
            [taoensso.timbre :as log]
            [datomic.api :as d]
            [datomic-blockchain.permission.specs :as specs]
            [datomic-blockchain.permission.checker :as checker]
            [datomic-blockchain.permission.policy :as policy])
  (:import [java.util UUID]))

;; ============================================================================
;; Helper Functions (must be declared before use)
;; ============================================================================

(defn extract-resource-ids
  "Extract resource IDs from transaction data
  Looks for :db/id and resource IDs in the data"
  [tx-data]
  (reduce (fn [acc item]
            (cond
              ;; Direct entity ID
              (and (map? item) (:db/id item))
              (conj acc (:db/id item))

              ;; Nested resource references
              (and (map? item) (:resource-id item))
              (conj acc (:resource-id item))

              :else
              acc))
          []
          tx-data))

(defn check-all-permissions
  "Check permissions for multiple resources
  Returns {:success? boolean :failed-ids [resource-ids]}"
  [conn request resource-ids action]
  (log/debug "Checking permissions for" (count resource-ids) "resources")
  (let [results (mapv #(policy/check-access
                         %
                         (assoc request :requested-action action))
                      resource-ids)
        failed (mapv :resource-id (filter (complement :success?) results))]
    (if (empty? failed)
      {:success? true}
      {:success? false
       :failed-ids failed
       :reason "Permission denied for one or more resources"})))

;; ============================================================================
;; Query Filtering with Permissions
;; ============================================================================

(defn filter-query-results
  "Filter Datomic query results based on permissions
  Returns only results accessible to the requestor"
  [db request results resource-id-key]
  (log/debug "Filtering" (count results) "results for requestor:" (:requestor-id request))
  (filterv (fn [result]
             (let [resource-id (get result resource-id-key)
                   check-result (policy/check-access resource-id request)]
               (when (:success? check-result)
                 (policy/log-access check-result))
               (:success? check-result)))
           results))

(defn filter-entity-query
  "Filter entity query results by permission
  Example: (filter-entity-query db request entity-ids)"
  [db request entity-ids]
  (log/debug "Filtering" (count entity-ids) "entities")
  (filterv (fn [entity-id]
             (let [result (policy/check-access entity-id request)]
               (:success? result)))
           entity-ids))

;; ============================================================================
;; Datomic Query Wrappers with Permission Check
;; ============================================================================

(defn q-with-permissions
  "Execute Datomic query with permission filtering
  Automatically filters results based on requestor's permissions"
  [db request query & args]
  (log/debug "Executing query with permission checks")
  (let [results (apply d/q query db args)]
    (if (vector? results)
      ;; If results are vectors, filter them
      (let [entity-ids (mapv first results)]  ; Assume first element is entity ID
        (filter-entity-query db request entity-ids))
      ;; Single result
      results)))

(defn entity-with-permission-check
  "Get entity only if requestor has permission
  Returns nil if not authorized"
  [db request entity-id]
  (log/debug "Getting entity with permission check:" entity-id)
  (if (policy/can-access? entity-id request)
    (d/entity db entity-id)
    (do
      (log/warn "Unauthorized access attempt to entity:" entity-id "by" (:requestor-id request))
      nil)))

;; ============================================================================
;; Transaction with Permission Check
;; ============================================================================

(defn transact-with-permission!
  "Transact data only if requestor has permission
  Returns {:success? boolean :result tx-result :error error}
  Ensures transaction is atomic and permission-checked"
  [conn request tx-data]
  (log/info "Transacting with permission check:" (count tx-data) "operations")
  (let [;; Check permissions for all resources in transaction
        resource-ids (extract-resource-ids tx-data)
        permissions-check (check-all-permissions conn request resource-ids (:requested-action request))]

    (if (:success? permissions-check)
      (try
        (let [tx-result @(d/transact conn tx-data)]
          (log/info "Transaction successful")
          {:success? true
           :result tx-result})
        (catch Exception e
          (log/error "Transaction failed:" e)
          {:success? false
           :error (.getMessage e)}))
      {:success? false
       :error "Permission denied"
       :reason permissions-check})))

;; ============================================================================
;; Middleware-Style Enforcement
;; ============================================================================

(defn wrap-permission-check
  "Middleware wrapper for functions that need permission checks
  Returns a function that checks permission before calling f
  Usage: (defn-protected (wrap-permission-check my-fn))"
  [f resource-id-fn]
  (fn [conn request & args]
    (log/debug "Permission wrapper for function:" f)
    (let [resource-id (apply resource-id-fn args)
          check-result (policy/check-access resource-id request)]
      (if (:success? check-result)
        (do
          (policy/log-access check-result)
          (apply f conn request args))
        (do
          (log/warn "Permission denied for function:" f)
          {:success? false
           :error "Permission denied"})))))

;; ============================================================================
;; Bulk Operations with Permission Filtering
;; ============================================================================

(defn bulk-transact-with-permissions!
  "Transact multiple entities, filtering out unauthorized ones
  Returns {:authorized [tx-data] :unauthorized [tx-data] :results}"
  [conn request tx-data-list]
  (log/info "Bulk transacting" (count tx-data-list) "entities with permission checks")
  (let [;; Separate authorized from unauthorized
        grouped (group-by (fn [tx-data]
                           (let [resource-ids (extract-resource-ids [tx-data])]
                             (if (every? #(policy/can-access? % request) resource-ids)
                               :authorized
                               :unauthorized)))
                         tx-data-list)

        authorized (get grouped :authorized [])
        unauthorized (get grouped :unauthorized [])]

    (log/info "Authorized:" (count authorized) "Unauthorized:" (count unauthorized))

    ;; Transact only authorized
    (if (seq authorized)
      {:authorized authorized
       :unauthorized unauthorized
       :results @(d/transact conn authorized)
       :success? true}
      {:authorized []
       :unauthorized unauthorized
       :results nil
       :success? false
       :error "No authorized transactions"})))

;; ============================================================================
;; Permission Cache for Performance with TTL
;; ============================================================================

(defrecord CacheEntry [value expires-at])

(def ^:private cache-ttl-ms
  "Default TTL for permission cache entries in milliseconds (5 minutes)
   Can be overridden via PERMISSION_CACHE_TTL_MS environment variable"
  (or (some-> (System/getenv "PERMISSION_CACHE_TTL_MS") Long/parseLong)
      300000))  ; 5 minutes default

(def ^:private permission-cache (atom {}))

(defn cached-check-access
  "Check access with caching and TTL for performance
   Cache key: [resource-id requestor-id action]
   Entries expire after cache-ttl-ms (default 5 minutes)"
  [resource-id request]
  (let [cache-key [resource-id (:requestor-id request) (:requested-action request)]
        now (System/currentTimeMillis)]
    (if-let [entry (get @permission-cache cache-key)]
      ;; Check if expired
      (if (> (:expires-at entry) now)
        (do
          (log/debug "Cache hit for:" cache-key)
          (:value entry))
        (do
          (log/debug "Cache expired for:" cache-key)
          (swap! permission-cache dissoc cache-key)
          ;; Fetch fresh result
          (let [result (policy/check-access resource-id request)]
            (swap! permission-cache assoc cache-key
                   (->CacheEntry result (+ now cache-ttl-ms)))
            result)))
      ;; Cache miss - fetch and cache
      (do
        (log/debug "Cache miss for:" cache-key)
        (let [result (policy/check-access resource-id request)]
          (swap! permission-cache assoc cache-key
                 (->CacheEntry result (+ now cache-ttl-ms)))
          result)))))

(defn clear-permission-cache!
  "Clear the permission cache"
  []
  (log/debug "Clearing permission cache")
  (reset! permission-cache {}))

(defn invalidate-cache-for-resource
  "Invalidate cache entries for a specific resource"
  [resource-id]
  (log/debug "Invalidating cache for resource:" resource-id)
  (swap! permission-cache
         (fn [cache]
           (apply dissoc cache
                  (filter #(= (first %) resource-id) (keys cache))))))

(defn get-cache-stats
  "Get statistics about the permission cache
   Returns map with :total-entries, :expired-entries (at check time)"
  []
  (let [now (System/currentTimeMillis)
        entries @permission-cache
        total (count entries)
        expired (count (filter #(<= (:expires-at (val %)) now) entries))]
    {:total-entries total
     :expired-entries expired
     :ttl-ms cache-ttl-ms}))

;; ============================================================================
;; Audit Trail Integration
;; ============================================================================

(defn audit-access
  "Create an audit entry for access attempt
  Automatically called by enforcement functions"
  [access-result]
  (policy/log-access (assoc access-result :access-granted (:success? access-result))))

;; ============================================================================
;; Helper Functions
;; ============================================================================

(defn require-permission
  "Throw exception if permission not granted
  Useful for assertion-style permission checks"
  [resource-id request]
  (let [result (policy/check-access resource-id request)]
    (when-not (:success? result)
      (throw (ex-info "Permission denied"
                      (merge result
                             {:resource-id resource-id
                              :requestor-id (:requestor-id request)}))))
    result))

(defn with-permission
  "Execute function only if permission granted
  Returns {:success? boolean :result result :error error}"
  [resource-id request f]
  (try
    (require-permission resource-id request)
    {:success? true
     :result (f)}
    (catch Exception e
      {:success? false
       :error (.getMessage e)})))

;; ============================================================================
;; Development Helpers
;; ============================================================================

(comment
  ;; Usage examples

  ;; Filter query results
  (def db (dev/db))
  (def request (specs/generate-access-request))
  (def results [[id1 data1] [id2 data2]])

  (filter-query-results db request results 0)

  ;; Get entity with permission check
  (entity-with-permission-check db request some-entity-id)

  ;; Transact with permission
  (transact-with-permission! (dev/conn)
                            request
                            [{:db/id "new"
                              :prov/entity (random-uuid)
                              :prov/entity-type :product/batch}])

  ;; Wrap function with permission check
  (def my-protected-fn
    (wrap-permission-check
     (fn [conn request entity-id]
       (d/entity (d/db conn) entity-id))
     identity))

  ;; Cached permission check
  (cached-check-access resource-id request)
  (clear-permission-cache!)
  (invalidate-cache-for-resource resource-id))
