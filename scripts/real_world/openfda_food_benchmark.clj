(ns real-world.openfda-food-benchmark
  "Real-world openFDA Food Enforcement benchmark.

  This harness fetches public FDA food enforcement records and maps source
  records into the existing Datomic PROV-O/traceability schema. It does not
  synthesize supply-chain paths or benchmark results: raw JSON is preserved,
  record identifiers come from openFDA, and internal UUIDs are deterministic
  derivatives of source identifiers for Datomic identity attributes."
  (:require [clj-http.client :as http]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [datomic.api :as d]
            [datomic-blockchain.datomic.schema :as schema])
  (:import [java.io File]
           [java.text SimpleDateFormat]
           [java.util Date UUID]))

(def endpoint "https://api.fda.gov/food/enforcement.json")
(def max-page-size 1000)

(def query-defs
  [{:name "q1_recall_number_lookup"
    :description "Exact lookup by FDA recall_number stored as :traceability/batch"}
   {:name "q2_entity_uuid_lookup"
    :description "Exact lookup by deterministic :prov/entity UUID derived from recall_number"}
   {:name "q3_activity_uuid_lookup"
    :description "Exact lookup by deterministic :prov/activity UUID derived from event_id + recall_number"}
   {:name "q4_recalling_firm_lookup"
    :description "Exact indexed recalling-firm lookup via :prov/agent-name"}])

(defn getenv
  [k default]
  (or (System/getenv k) default))

(defn parse-long-env
  [k default]
  (Long/parseLong (getenv k (str default))))

(defn ensure-dir!
  [path]
  (.mkdirs (io/file path)))

(defn stable-uuid
  [s]
  (UUID/nameUUIDFromBytes (.getBytes (str s) "UTF-8")))

(def ^:private yyyymmdd
  (doto (SimpleDateFormat. "yyyyMMdd")
    (.setLenient false)))

(defn parse-date
  [s]
  (when (and (string? s) (re-matches #"\d{8}" s))
    (try
      (.parse yyyymmdd s)
      (catch Exception _ nil))))

(defn compact-location
  [record]
  (let [parts [(get record :city)
               (get record :state)
               (get record :country)]]
    (or (some->> parts
                 (remove str/blank?)
                 seq
                 (str/join ", "))
        "Unknown")))

(defn usable-record?
  [record]
  (and (not (str/blank? (get record :recall_number)))
       (not (str/blank? (get record :event_id)))
       (not (str/blank? (get record :recalling_firm)))
       (not (str/blank? (get record :product_description)))))

(defn fetch-page
  [{:keys [limit skip search]}]
  (let [params (cond-> {"limit" limit
                        "skip" skip}
                 (not (str/blank? search)) (assoc "search" search))
        response (http/get endpoint {:as :text
                                     :accept :json
                                     :query-params params})
        body (:body response)]
    (json/read-str body :key-fn keyword)))

(defn fetch-records
  [{:keys [limit search]}]
  (loop [skip 0
         remaining limit
         pages []
         records []
         meta nil]
    (if (pos? remaining)
      (let [page-limit (min max-page-size remaining)
            page (fetch-page {:limit page-limit :skip skip :search search})
            page-records (:results page)
            fetched (count page-records)]
        (if (zero? fetched)
          {:meta meta :pages pages :records records}
          (recur (+ skip fetched)
                 (- remaining fetched)
                 (conj pages page)
                 (into records page-records)
                 (:meta page))))
      {:meta meta :pages pages :records records})))

(defn write-json!
  [path value]
  (spit path (json/write-str value :escape-slash false)))

(defn record->ids
  [record]
  (let [recall-number (:recall_number record)
        event-id (:event_id record)
        firm (:recalling_firm record)
        agent-location (compact-location record)]
    {:entity-id (stable-uuid (str "openfda-food/entity/" recall-number))
     :activity-id (stable-uuid (str "openfda-food/activity/" event-id "/" recall-number))
     :agent-id (stable-uuid (str "openfda-food/agent/" firm "/" agent-location))}))

(defn record->tx
  [record]
  (let [{:keys [entity-id activity-id agent-id]} (record->ids record)
        recall-number (:recall_number record)
        started-at (or (parse-date (:recall_initiation_date record))
                       (parse-date (:report_date record))
                       (Date. 0))
        ended-at (parse-date (:termination_date record))]
    (cond->
     [{:prov/agent agent-id
       :prov/agent-name (:recalling_firm record)
       :prov/agent-type :openfda.food/recalling-firm
       :traceability/location (compact-location record)}
      {:prov/entity entity-id
       :prov/entity-type :openfda.food/recalled-product
       :traceability/batch recall-number
       :traceability/product (stable-uuid (str "openfda-food/product/" recall-number))
       :traceability/product-name (:product_description record)
       :traceability/location (or (:distribution_pattern record)
                                  (compact-location record))}
      {:prov/activity activity-id
       :prov/activity-type :openfda.food/recall-event
       :prov/startedAtTime started-at
       :prov/used [entity-id]
       :prov/wasAssociatedWith [agent-id]}]
      ended-at (update 2 assoc :prov/endedAtTime ended-at))))

(defn transact-records!
  [conn records]
  (doseq [chunk (partition-all 250 (mapcat record->tx records))]
    @(d/transact conn (vec chunk))))

(defn datomic-uri
  [{:keys [storage db-name host port uri]}]
  (case storage
    "mem" (str "datomic:mem://" db-name)
    "dev-transactor" (format "datomic:dev://%s:%s/%s" host port db-name)
    "uri" uri
    (throw (ex-info "Unsupported DATOMIC_STORAGE"
                    {:storage storage
                     :supported ["mem" "dev-transactor" "uri"]}))))

(defn setup-db!
  [datomic-config]
  (let [uri (datomic-uri datomic-config)]
    (d/create-database uri)
    (let [conn (d/connect uri)]
      (schema/install-schema conn)
      conn)))

(defn now-ms
  []
  (System/nanoTime))

(defn elapsed-ms
  [start-ns]
  (/ (double (- (System/nanoTime) start-ns)) 1000000.0))

(defn query-once
  [db query-name anchors]
  (case query-name
    "q1_recall_number_lookup"
    (d/q '[:find ?e .
           :in $ ?recall
           :where [?e :traceability/batch ?recall]]
         db (:recall-number anchors))

    "q2_entity_uuid_lookup"
    (d/q '[:find ?e .
           :in $ ?entity-id
           :where [?e :prov/entity ?entity-id]]
         db (:entity-id anchors))

    "q3_activity_uuid_lookup"
    (d/q '[:find ?a .
           :in $ ?activity-id
           :where [?a :prov/activity ?activity-id]]
         db (:activity-id anchors))

    "q4_recalling_firm_lookup"
    (d/q '[:find (count ?activity) .
           :in $ ?firm
           :where
           [?agent :prov/agent-name ?firm]
           [?agent :prov/agent ?agent-id]
           [?activity :prov/wasAssociatedWith ?agent-id]]
         db (:recalling-firm anchors))))

(defn semantic-ok?
  [query-name result]
  (case query-name
    "q4_recalling_firm_lookup" (and (number? result) (pos? result))
    (some? result)))

(defn bench-query
  [conn {:keys [name]} anchors warmup reps]
  (dotimes [_ warmup]
    (query-once (d/db conn) name anchors))
  (mapv
   (fn [idx]
     (let [db (d/db conn)
           start (now-ms)
           result (query-once db name anchors)
           latency (elapsed-ms start)
           status (if (semantic-ok? name result) "ok" "semantic-error")]
       {:query name
        :iteration (inc idx)
        :status status
        :latency-ms latency}))
   (range reps)))

(defn percentile
  [sorted-values pct]
  (let [n (count sorted-values)
        idx (max 0 (dec (long (Math/ceil (* n (/ pct 100.0))))))]
    (nth sorted-values idx)))

(defn summarize
  [rows]
  (for [[query-name query-rows] (sort-by key (group-by :query rows))]
    (let [ok-rows (filter #(= "ok" (:status %)) query-rows)
          errors (- (count query-rows) (count ok-rows))
          vals (sort (map :latency-ms ok-rows))
          n (count vals)]
      (if (pos? n)
        {:query query-name
         :ok-count n
         :error-count errors
         :mean-ms (/ (reduce + vals) n)
         :p50-ms (percentile vals 50)
         :p95-ms (percentile vals 95)
         :p99-ms (percentile vals 99)
         :min-ms (first vals)
         :max-ms (last vals)}
        {:query query-name
         :ok-count 0
         :error-count errors
         :mean-ms nil
         :p50-ms nil
         :p95-ms nil
         :p99-ms nil
         :min-ms nil
         :max-ms nil}))))

(defn format-ms
  [v]
  (if (number? v) (format "%.3f" (double v)) "NA"))

(defn write-raw-csv!
  [path rows]
  (with-open [w (io/writer path)]
    (.write w "query,iteration,status,latency_ms\n")
    (doseq [{:keys [query iteration status latency-ms]} rows]
      (.write w (format "%s,%d,%s,%s\n"
                        query iteration status (format-ms latency-ms))))))

(defn write-summary-csv!
  [path rows]
  (with-open [w (io/writer path)]
    (.write w "query,ok_count,error_count,mean_ms,p50_ms,p95_ms,p99_ms,min_ms,max_ms\n")
    (doseq [{:keys [query ok-count error-count mean-ms p50-ms p95-ms p99-ms min-ms max-ms]} rows]
      (.write w (format "%s,%d,%d,%s,%s,%s,%s,%s,%s\n"
                        query ok-count error-count
                        (format-ms mean-ms)
                        (format-ms p50-ms)
                        (format-ms p95-ms)
                        (format-ms p99-ms)
                        (format-ms min-ms)
                        (format-ms max-ms))))))

(defn write-manifest!
  [path {:keys [run-id out-dir limit search warmup reps git-commit git-dirty
                datomic-config ingest-ms meta fetched usable anchors summary total-errors]}]
  (let [results-meta (:results meta)]
    (spit path
          (str
           "run_id=" run-id "\n"
           "timestamp_utc=" (java.time.Instant/now) "\n"
           "source=openFDA Food Enforcement Reports\n"
           "source_endpoint=" endpoint "\n"
           "source_terms=https://open.fda.gov/terms/\n"
           "source_license=https://open.fda.gov/license/ (CC0 unless otherwise noted)\n"
           "source_last_updated=" (:last_updated meta) "\n"
           "source_total_available=" (:total results-meta) "\n"
           "limit=" limit "\n"
           "search=" (or search "") "\n"
           "fetched_records=" fetched "\n"
           "usable_records=" usable "\n"
           "warmup=" warmup "\n"
           "reps=" reps "\n"
           "datomic_storage=" (:storage datomic-config) "\n"
           "datomic_db_name=" (:db-name datomic-config) "\n"
           "datomic_host=" (:host datomic-config) "\n"
           "datomic_port=" (:port datomic-config) "\n"
           "ingest_ms=" (format-ms ingest-ms) "\n"
           "git_commit=" git-commit "\n"
           "git_dirty=" git-dirty "\n"
           "out_dir=" out-dir "\n"
           "mapping=real openFDA records mapped to PROV entities/activities; UUIDs are deterministic derivatives of source IDs\n"
           "selected_recall_number=" (:recall-number anchors) "\n"
           "selected_event_id=" (:event-id anchors) "\n"
           "selected_recalling_firm=" (:recalling-firm anchors) "\n"
           "selected_entity_id=" (:entity-id anchors) "\n"
           "selected_activity_id=" (:activity-id anchors) "\n"
           "selected_agent_id=" (:agent-id anchors) "\n"
           "total_errors=" total-errors "\n"
           "summary_queries=" (str/join "," (map :query summary)) "\n"))))

(defn run-benchmark!
  []
  (let [run-id (getenv "RUN_ID" (str "openfda_food_" (.format (SimpleDateFormat. "yyyyMMdd_HHmmss") (Date.))))
        out-dir (getenv "OUT_DIR" (str "benchmarks/real-world/results/" run-id))
        limit (parse-long-env "LIMIT" 1000)
        warmup (parse-long-env "WARMUP" 30)
        reps (parse-long-env "REPS" 100)
        git-commit (getenv "GIT_COMMIT" "unknown")
        git-dirty (getenv "GIT_DIRTY" "unknown")
        datomic-config {:storage (getenv "DATOMIC_STORAGE" "mem")
                        :db-name (getenv "DATOMIC_DB_NAME" (str "openfda-food-" run-id))
                        :host (getenv "DATOMIC_HOST" "localhost")
                        :port (parse-long-env "DATOMIC_PORT" 4334)
                        :uri (System/getenv "DATOMIC_URI")}
        search (System/getenv "OPENFDA_SEARCH")
        _ (ensure-dir! out-dir)
        fetched (fetch-records {:limit limit :search search})
        raw-path (str out-dir "/openfda_food_raw.json")
        _ (write-json! raw-path fetched)
        records (filterv usable-record? (:records fetched))]
    (when (empty? records)
      (throw (ex-info "No usable openFDA records returned" {:limit limit :search search})))
    (let [conn (setup-db! datomic-config)
          ingest-start (now-ms)
          _ (transact-records! conn records)
          ingest-ms (elapsed-ms ingest-start)
          selected (first records)
          ids (record->ids selected)
          anchors {:recall-number (:recall_number selected)
                   :event-id (:event_id selected)
                   :recalling-firm (:recalling_firm selected)
                   :entity-id (:entity-id ids)
                   :activity-id (:activity-id ids)
                   :agent-id (:agent-id ids)}
          raw-rows (mapcat #(bench-query conn % anchors warmup reps) query-defs)
          summary (vec (summarize raw-rows))
          total-errors (reduce + (map :error-count summary))
          raw-csv (str out-dir "/openfda_food_latency_raw.csv")
          summary-csv (str out-dir "/openfda_food_latency_summary.csv")
          manifest (str out-dir "/manifest.txt")]
      (write-raw-csv! raw-csv raw-rows)
      (write-summary-csv! summary-csv summary)
      (write-manifest! manifest {:run-id run-id
                                 :out-dir out-dir
                                 :limit limit
                                 :search search
                                 :warmup warmup
                                 :reps reps
                                 :git-commit git-commit
                                 :git-dirty git-dirty
                                 :datomic-config datomic-config
                                 :ingest-ms ingest-ms
                                 :meta (:meta fetched)
                                 :fetched (count (:records fetched))
                                 :usable (count records)
                                 :anchors anchors
                                 :summary summary
                                 :total-errors total-errors})
      (println "openFDA real-world benchmark complete")
      (println "Raw JSON:" raw-path)
      (println "Raw CSV:" raw-csv)
      (println "Summary:" summary-csv)
      (println "Manifest:" manifest)
      (when (pos? total-errors)
        (System/exit 2)))))

(defn -main
  [& _args]
  (try
    (run-benchmark!)
    (shutdown-agents)
    (System/exit 0)
    (catch Throwable t
      (.printStackTrace t)
      (shutdown-agents)
      (System/exit 1))))
