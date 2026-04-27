(ns datomic-blockchain.api.metrics
  "Prometheus-compatible metrics collection and reporting
   
   Provides HTTP endpoints for metrics scraping and in-code metrics
   collection for monitoring the blockchain system.
   
   Usage:
     (require '[datomic-blockchain.api.metrics :as metrics])
     
     ;; Record a metric
     (metrics/inc! :http/requests-total {:method :get :status 200})
     (metrics/observe! :http/request-duration-seconds {:method :post} 0.023)
     
     ;; Get metrics for Prometheus
     (metrics/prometheus-format)
   "
  (:require [clojure.string :as str]
            [taoensso.timbre :as log])
  (:import [java.time Instant Duration]))

;; ============================================================================
;; Metrics Storage
;; ============================================================================

(defonce ^:private metrics-store
  (atom {:counters {}
         :gauges {}
         :histograms {}
         :summaries {}}))

(defonce ^:private metric-metadata
  (atom {}))

;; ============================================================================
;; Metric Types
;; ============================================================================

(defn register-counter
  "Register a counter metric.
   
   Counters are cumulative metrics that can only increase.
   Use for: request counts, errors, tasks completed.
   
   Example:
     (register-counter :http/requests-total
                       {:description \"Total HTTP requests\"}
                       [:method :status])"
  [metric-name {:keys [description help]} label-names]
  (swap! metric-metadata assoc metric-name
         {:type :counter
          :description (or description help)
          :label-names (set label-names)})
  (swap! metrics-store assoc-in [:counters metric-name] {}))

(defn register-gauge
  "Register a gauge metric.
   
   Gauges can go up and down.
   Use for: current memory usage, queue depth, temperature.
   
   Example:
     (register-gauge :blockchain/block-height
                     {:description \"Current block height\"})"
  [metric-name {:keys [description help]}]
  (swap! metric-metadata assoc metric-name
         {:type :gauge
          :description (or description help)})
  (swap! metrics-store assoc-in [:gauges metric-name] 0))

(defn register-histogram
  "Register a histogram metric.
   
   Histograms sample observations and count them in buckets.
   Use for: request latencies, response sizes.
   
   Example:
     (register-histogram :http/request-duration-seconds
                         {:description \"Request latency\"}
                         [:method]
                         [0.005 0.01 0.025 0.05 0.1 0.25 0.5 1 2.5 5 10])"
  [metric-name {:keys [description help]} label-names buckets]
  (swap! metric-metadata assoc metric-name
         {:type :histogram
          :description (or description help)
          :label-names (set label-names)
          :buckets buckets})
  (swap! metrics-store assoc-in [:histograms metric-name] {}))

;; ============================================================================
;; Metric Operations
;; ============================================================================

(defn- label-key
  "Convert label map to sorted string key for storage"
  [labels]
  (->> labels
       (into (sorted-map))
       (map (fn [[k v]] (str (name k) "=\"" v "\"")))
       (str/join ",")))

(defn inc!
  "Increment a counter metric.
   
   Example:
     (inc! :http/requests-total)
     (inc! :http/requests-total {:method :get :status 200})"
  ([metric-name]
   (inc! metric-name {}))
  ([metric-name labels]
   (swap! metrics-store update-in [:counters metric-name (label-key labels)]
          (fnil inc 0))))

(defn dec!
  "Decrement a counter (useful for gauges that go down)."
  ([metric-name]
   (dec! metric-name {}))
  ([metric-name labels]
   (swap! metrics-store update-in [:counters metric-name (label-key labels)]
          (fnil dec 0))))

(defn set-gauge!
  "Set a gauge to a specific value."
  [metric-name value]
  (swap! metrics-store assoc-in [:gauges metric-name] value))

(defn observe!
  "Observe a value in a histogram.
   
   Example:
     (observe! :http/request-duration-seconds {:method :get} 0.023)"
  [metric-name labels value]
  (let [lkey (label-key labels)]
    (swap! metrics-store update-in [:histograms metric-name lkey :sum]
           (fnil + 0) value)
    (swap! metrics-store update-in [:histograms metric-name lkey :count]
           (fnil inc 0))
    ;; Track bucket counts
    (let [buckets (get-in @metric-metadata [metric-name :buckets])
          bucket-counts (get-in @metrics-store [:histograms metric-name lkey :buckets] {})]
      (doseq [bucket buckets]
        (when (<= value bucket)
          (swap! metrics-store update-in [:histograms metric-name lkey :buckets bucket]
                 (fnil inc 0)))))))

;; ============================================================================
;; Timing Helpers
;; ============================================================================

(defmacro with-timing
  "Execute body and record duration in histogram.
   
   Example:
     (with-timing :http/request-duration-seconds {:method :get}
       (handle-request request))"
  [metric-name labels & body]
  `(let [start# (System/nanoTime)
         result# (do ~@body)
         duration# (/ (- (System/nanoTime) start#) 1e9)]
     (observe! ~metric-name ~labels duration#)
     result#))

;; ============================================================================
;; Default Metrics Registration
;; ============================================================================

(defn register-default-metrics
  "Register standard metrics for the blockchain system."
  []
  ;; HTTP metrics
  (register-counter :http/requests-total
                    {:description "Total HTTP requests"}
                    [:method :status :handler])
  
  (register-histogram :http/request-duration-seconds
                      {:description "HTTP request latency"}
                      [:method :handler]
                      [0.001 0.005 0.01 0.025 0.05 0.1 0.25 0.5 1 2.5 5 10])
  
  ;; Blockchain metrics
  (register-gauge :blockchain/block-height
                  {:description "Current blockchain height"})
  
  (register-counter :blockchain/transactions-total
                    {:description "Total transactions processed"}
                    [:status])
  
  (register-histogram :blockchain/transaction-size-bytes
                      {:description "Transaction size in bytes"}
                      []
                      [100 500 1000 5000 10000 50000 100000])
  
  ;; Consensus metrics
  (register-counter :consensus/proposals-total
                    {:description "Total consensus proposals"}
                    [:status])
  
  (register-histogram :consensus/vote-duration-seconds
                      {:description "Time to reach consensus"}
                      [:protocol]
                      [0.01 0.025 0.05 0.1 0.25 0.5 1 2.5 5])
  
  ;; Graph query metrics
  (register-counter :graph/queries-total
                    {:description "Total graph queries"}
                    [:type :status])
  
  (register-histogram :graph/query-duration-seconds
                      {:description "Graph query latency"}
                      [:type]
                      [0.001 0.005 0.01 0.025 0.05 0.1 0.25 0.5 1 2.5])
  
  ;; Permission metrics
  (register-counter :permission/checks-total
                    {:description "Total permission checks"}
                    [:result])
  
  (log/info "Default metrics registered"))

;; Register on namespace load
(register-default-metrics)

;; ============================================================================
;; Prometheus Format Export
;; ============================================================================

(defn- format-labels
  "Format labels for Prometheus text format"
  [labels]
  (if (seq labels)
    (str "{" (str/join "," (map (fn [[k v]] (str (name k) "=\"" v "\"")) labels)) "}")
    ""))

(defn- format-metric-line
  "Format a single metric line"
  [name labels value]
  (str name (format-labels labels) " " value))

(defn- export-counter
  "Export counter metrics in Prometheus format"
  [name data metadata]
  (let [description (:description metadata)]
    (str "# HELP " name " " description "\n"
         "# TYPE " name " counter\n"
         (str/join "\n"
                   (map (fn [[labels value]]
                          (format-metric-line name
                                              (if (seq labels)
                                                (into {} (map #(let [[k v] (str/split % #"=")]
                                                                  [(keyword k) (str/replace v #"\"" "")])
                                                              (str/split labels #",")))
                                                {})
                                              value))
                        data))
         "\n")))

(defn- export-gauge
  "Export gauge metrics in Prometheus format"
  [name value metadata]
  (let [description (:description metadata)]
    (str "# HELP " name " " description "\n"
         "# TYPE " name " gauge\n"
         name " " value "\n")))

(defn- export-histogram
  "Export histogram metrics in Prometheus format"
  [name data metadata]
  (let [description (:description metadata)
        buckets (:buckets metadata)]
    (str "# HELP " name " " description "\n"
         "# TYPE " name " histogram\n"
         ;; TODO: Implement full histogram export
         "\n")))

(defn prometheus-format
  "Export all metrics in Prometheus text format.
   
   This format can be scraped by Prometheus or other compatible systems."
  []
  (let [store @metrics-store
        metadata @metric-metadata]
    (str/join "\n"
              (concat
               ;; Counters
               (map (fn [[name data]]
                      (when-let [meta (get metadata name)]
                        (export-counter name data meta)))
                    (:counters store))
               
               ;; Gauges
               (map (fn [[name value]]
                      (when-let [meta (get metadata name)]
                        (export-gauge name value meta)))
                    (:gauges store))
               
               ;; Histograms
               (map (fn [[name data]]
                      (when-let [meta (get metadata name)]
                        (export-histogram name data meta)))
                    (:histograms store))))))

;; ============================================================================
;; HTTP Handler
;; ============================================================================

(defn handle-metrics
  "HTTP handler for /metrics endpoint.
   
   Returns metrics in Prometheus text format."
  [_request]
  {:status 200
   :headers {"Content-Type" "text/plain; version=0.0.4; charset=utf-8"}
   :body (prometheus-format)})

;; ============================================================================
;; Middleware
;; ============================================================================

(defn wrap-metrics
  "Ring middleware to collect HTTP metrics.
   
   Wraps handlers to automatically record:
   - Request count by method, status, handler
   - Request duration histogram
   
   Usage:
     (-> handler
         (wrap-metrics :api-handler))"
  [handler handler-name]
  (fn [request]
    (let [start (System/nanoTime)
          method (-> request :request-method name str/upper-case)
          response (handler request)
          duration (/ (- (System/nanoTime) start) 1e9)
          status (:status response 200)]
      
      ;; Record metrics
      (inc! :http/requests-total
            {:method method :status (str status) :handler (name handler-name)})
      
      (observe! :http/request-duration-seconds
                {:method method :handler (name handler-name)}
                duration)
      
      response)))

(comment
  ;; Example usage
  (inc! :http/requests-total {:method "GET" :status "200" :handler "blocks"})
  (inc! :blockchain/transactions-total {:status "success"})
  (set-gauge! :blockchain/block-height 42)
  (observe! :http/request-duration-seconds {:method "GET" :handler "blocks"} 0.023)
  
  ;; View current metrics
  (prometheus-format)
  
  ;; Reset all metrics
  (reset! metrics-store {:counters {} :gauges {} :histograms {} :summaries {}})
  )
