(ns datomic-blockchain.api.exceptions
  "Custom exception hierarchy for specific error handling

   Provides typed exceptions for different error domains:
   - DatomicException: Database-related errors
   - ValidationException: Input validation failures
   - ConsensusException: Cluster consensus errors
   - PermissionException: Access control errors
   - QueryException: Query execution errors

   Usage:
     (throw (ex-info \"Validation failed\"
             {:error/type :validation
              :field :user-id
              :value \"invalid\"}))
     (catch Exception e
       (if (= :validation (-> (ex-data e) :error/type))
         (handle-validation e)))")

;; ============================================================================
;; Exception Type Helpers
;; ============================================================================

(defn datomic-exception
  "Create an ex-info for Datomic/database errors
   Options:
     :cause - underlying exception
     :entity - related entity ID
     :operation - operation that failed"
  [message & [opts]]
  (ex-info message
          (merge {:error/type :datomic
                  :category :database}
                 opts)
          (:cause opts)))

(defn validation-exception
  "Create an ex-info for validation errors
   Options:
     :field - field that failed validation
     :value - the invalid value
     :constraint - validation constraint that failed"
  [message & [opts]]
  (ex-info message
          (merge {:error/type :validation
                  :category :input-validation}
                 (or opts {}))
          nil))

(defn consensus-exception
  "Create an ex-info for consensus errors
   Options:
     :proposal-id - related proposal ID
     :node-id - node ID
     :phase - consensus phase (preprepare/prepare/commit)"
  [message & [opts]]
  (ex-info message
          (merge {:error/type :consensus
                  :category :cluster}
                 (or opts {}))
          nil))

(defn permission-exception
  "Create an ex-info for permission/access errors
   Options:
     :resource-id - resource being accessed
     :requestor-id - user/requestor ID
     :action - requested action"
  [message & [opts]]
  (ex-info message
          (merge {:error/type :permission
                  :category :authorization}
                 (or opts {}))
          nil))

(defn query-exception
  "Create an ex-info for query errors
   Options:
     :query - the query that failed
     :params - query parameters"
  [message & [opts]]
  (ex-info message
          (merge {:error/type :query
                  :category :database}
                 (or opts {}))
          nil))

;; ============================================================================
;; Exception Predicates
;; ============================================================================

(defn datomic-exception?
  "Check if exception data indicates a DatomicException"
  [e]
  (= :datomic (-> (ex-data e) :error/type)))

(defn validation-exception?
  "Check if exception data indicates a ValidationException"
  [e]
  (= :validation (-> (ex-data e) :error/type)))

(defn consensus-exception?
  "Check if exception data indicates a ConsensusException"
  [e]
  (= :consensus (-> (ex-data e) :error/type)))

(defn permission-exception?
  "Check if exception data indicates a PermissionException"
  [e]
  (= :permission (-> (ex-data e) :error/type)))

(defn query-exception?
  "Check if exception data indicates a QueryException"
  [e]
  (= :query (-> (ex-data e) :error/type)))

;; ============================================================================
;; Error Category Helpers
;; ============================================================================

(defn exception-category
  "Categorize exception for handling/logging purposes
   Returns: :datomic, :validation, :consensus, :permission, :query, :unknown"
  [e]
  (or (some-> (ex-data e) :error/type)
      :unknown))

(defn recoverable?
  "Check if exception is recoverable (validation, permission, query) vs fatal (datomic, consensus)"
  [e]
  (contains? #{:validation :permission :query} (exception-category e)))

(defn exception-summary
  "Create a summary of the exception for logging
   Includes exception type, category, and key fields"
  [e]
  (let [data (ex-data e)]
    (merge
     {:exception/type (exception-category e)
      :message (.getMessage e)
      :recoverable (recoverable? e)}
     (select-keys data [:field :value :resource-id :requestor-id :proposal-id :query]))))
