(ns datomic-blockchain.api.handlers.common
  "Common utilities and validation for API handlers"
  (:require [clojure.string :as str]
            [clojure.data.json :as json]
            [taoensso.timbre :as log]
            [datomic-blockchain.api.middleware :as middleware])
  (:import [java.util UUID Date]))

;; ============================================================================
;; Constants
;; ============================================================================

(def ^:private max-page-size
  "Maximum number of items per page to prevent excessive memory usage"
  1000)

(def ^:private default-page-size
  "Default number of items per page when not specified"
  10)

;; ============================================================================
;; Input Validation (SECURITY: Prevent injection and invalid data)
;; ============================================================================

(def ^:private uuid-regex
  "Regex for matching UUID format (8-4-4-4-12 hex digits)"
  #"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")

(defn valid-uuid?
  "Check if string is a valid UUID format"
  [s]
  (and (string? s)
       (re-matches uuid-regex s)))

(defn parse-uuid-safe
  "Safely parse a UUID string, returning nil if invalid"
  [s]
  (when (valid-uuid? s)
    (try
      (UUID/fromString s)
      (catch Exception _
        nil))))

(defn validate-uuid-param
  "Validate a UUID parameter from request.
   Returns UUID if valid, throws ex-info with error details if invalid.
   For use in handlers to provide meaningful error messages."
  [param-name param-value]
  (if (valid-uuid? param-value)
    (try
      (UUID/fromString param-value)
      (catch Exception e
        (throw (ex-info "Invalid UUID parameter"
                       {:error :invalid-uuid
                        :param param-name
                        :value param-value
                        :message (str "Parameter '" param-name "' is not a valid UUID")}
                       e))))
    (throw (ex-info "Invalid UUID format"
                   {:error :invalid-uuid-format
                    :param param-name
                    :value param-value
                    :message (str "Parameter '" param-name "' must be a valid UUID (format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)")}))))

(defn validate-positive-int
  "Validate and parse a positive integer parameter.
   Returns parsed int if valid, throws ex-info if invalid."
  [param-name param-value default]
  (let [value (or param-value (str default))]
    (try
      (let [parsed (Integer/parseInt value)]
        (if (>= parsed 0)
          parsed
          (throw (ex-info "Parameter must be non-negative"
                         {:error :invalid-range
                          :param param-name
                          :value value}))))
      (catch NumberFormatException e
        (throw (ex-info "Invalid integer parameter"
                       {:error :invalid-int
                        :param param-name
                        :value value}
                       e))))))

(defn sanitize-string-param
  "Sanitize string parameters to prevent injection.
   Returns nil if input is too long or contains suspicious patterns."
  [param-name value max-length]
  (when value
    (let [s (str value)]
      (cond
        ;; Check length
        (> (count s) max-length)
        (throw (ex-info "Parameter too long"
                       {:error :param-too-long
                        :param param-name
                        :max-length max-length
                        :actual-length (count s)}))

        ;; Check for potentially dangerous patterns (basic SQL/Script injection detection)
        (or (.contains s "<script")
            (.contains (str/lower-case s) "javascript:")
            (.contains s "onerror=")
            (.contains s "onload="))
        (do
          (log/warn "Suspicious pattern detected in parameter:" param-name)
          (throw (ex-info "Suspicious input detected"
                         {:error :suspicious-input
                          :param param-name})))

        ;; Return sanitized value
        :else
        s))))

;; ============================================================================
;; Pagination Helpers
;; ============================================================================

(defn parse-pagination
  "Parse pagination parameters from request"
  [request]
  (let [params (:params request)
        offset (or (some-> (get params "offset") parse-long) 0)
        limit (or (some-> (get params "limit") parse-long) default-page-size)
        limit (min max-page-size limit)]
    {:offset offset
     :limit limit}))

(defn paginated-response
  "Create a paginated response with items and pagination metadata"
  [items total {:keys [offset limit]}]
  {:items items
   :pagination {:total total
                :offset offset
                :limit limit
                :count (count items)
                :has-more? (< (+ offset limit) total)}})

;; ============================================================================
;; JSON Body Parsing
;; ============================================================================

(defn get-val
  "Helper to get value from map with string or keyword key"
  [m k]
  (or (k m) (get m (name k))))

(defn extract-body-params
  "Extract body parameters from request, handling various formats"
  [request]
  (let [raw-body (:body request)]
    (cond
      ;; First check if body-params has the keys we need
      (and (:body-params request)
           (seq (:body-params request)))
      (:body-params request)

      ;; Then check :params
      (and (:params request)
           (seq (:params request)))
      (:params request)

      ;; Check if raw-body is already a parsed map
      (map? raw-body)
      raw-body

      ;; Last resort: parse as JSON string
      (and raw-body (string? raw-body))
      (try
        (json/read-str raw-body :key-fn keyword)
        (catch Exception e
          (log/debug "Failed to parse JSON body:" (.getMessage e))
          nil))

      :else
      nil)))

(defn keywordize-body-params
  "Convert string keys in body params to keywords"
  [body-params]
  (if (map? body-params)
    (zipmap (map keyword (keys body-params)) (vals body-params))
    body-params))

;; ============================================================================
;; Standard Response Helpers
;; ============================================================================

(defn success
  "Create successful response"
  ([data] (middleware/success-response data 200))
  ([data status] (middleware/success-response data status)))

(defn error
  "Create error response"
  ([message] (middleware/error-response message 500))
  ([message status] (middleware/error-response message status))
  ([message status details]
   {:status status
    :headers {"Content-Type" "application/json"}
    :body (json/write-str
           {:success false
            :error message
            :details details
            :timestamp (str (Date.))})}))

(defn json-write-str
  "Write JSON string with date handling"
  [data]
  (json/write-str data {:date-fn #(.toString %)}))

(defn not-found
  "Create not found response"
  [resource-type id]
  (error (str resource-type " not found: " id) 404))

(defn validation-error
  "Create validation error response"
  [message details]
  (error message 400 details))

;; ============================================================================
;; Error Handling
;; ============================================================================

(defmacro with-error-handling
  "Wrap handler body with standardized error handling"
  [context & body]
  `(try
     ~@body
     (catch clojure.lang.ExceptionInfo e#
       (let [data# (ex-data e#)
             status# (or (:status data#) 400)]
         (log/warn ~context "validation error:" (:error data#))
         (error (:message data#) status# (dissoc data# :status))))
     (catch Exception e#
       (log/error ~context "error:" (.getMessage e#))
       (error "Internal server error" 500))))
