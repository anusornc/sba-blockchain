(ns datomic-blockchain.datomic.connection
  "Datomic connection management for blockchain with embedded ontology"
  (:require [datomic.api :as d]
            [taoensso.timbre :as log]
            [clojure.java.io :as io])
  (:import [java.io File]))

(defn- ensure-data-dir
  "Ensure the data directory exists for persistent storage"
  [path]
  (when path
    (let [dir (File. path)]
      (when-not (.exists dir)
        (.mkdirs dir)
        (log/info "Created data directory:" path)))))

(defn- build-uri
  "Build Datomic URI based on server type"
  [cfg]
  (let [server-type (get-in cfg [:datomic :server-type])
        db-name (get-in cfg [:datomic :db-name])
        port (get-in cfg [:datomic :port] 4334)
        uri (get-in cfg [:datomic :uri])]
    (case server-type
      :dev (format "datomic:mem://%s" db-name)
      :dev-transactor (format "datomic:dev://localhost:%s/%s" port db-name)
      :free (format "datomic:free://datomic:datomic@localhost:%s/%s" port db-name)
      :local (let [{:keys [subprotocol subname user password]} (:datomic cfg)]
               ;; Ensure data directory exists
               (ensure-data-dir "./data/datomic")
               (format "datomic:sql://%s?jdbc:%s:%s?user=%s&password=%s"
                       db-name subprotocol subname user password))
      :pro uri
      ;; Default to memory for unknown types
      (do (log/warn "Unknown server type:" server-type ", defaulting to memory")
          (format "datomic:mem://%s" db-name)))))

(defn create-database
  "Create Datomic database if it doesn't exist"
  [cfg]
  (let [db-name (get-in cfg [:datomic :db-name])
        server-type (get-in cfg [:datomic :server-type])]
    (log/info "Creating database:" db-name "(type:" server-type ")")
    (let [uri (build-uri cfg)]
      (log/info "Using URI:" (if (= :local server-type) 
                              "datomic:sql://<hidden>" 
                              uri))
      (try
        (d/create-database uri)
        (log/info "Database created successfully")
        (catch Exception e
          (if (re-find #"Database already exists" (.getMessage e))
            (log/info "Database already exists")
            (throw e)))))))

(defn delete-database
  "Delete Datomic database (useful for tests)"
  [cfg]
  (let [db-name (get-in cfg [:datomic :db-name])
        server-type (get-in cfg [:datomic :server-type])]
    (log/info "Deleting database:" db-name)
    (let [uri (build-uri cfg)]
      (try
        (d/delete-database uri)
        (log/info "Database deleted successfully")
        (catch Exception e
          (log/warn "Could not delete database:" (.getMessage e))
          false)))))

(defn recreate-database
  "Delete and recreate database (useful for tests)"
  [cfg]
  (delete-database cfg)
  (create-database cfg))

(defn connect
  "Connect to Datomic and return connection object
  Creates database if it doesn't exist"
  [cfg]
  (log/info "Connecting to Datomic...")
  (let [server-type (get-in cfg [:datomic :server-type])
        uri (build-uri cfg)]

    (case server-type
      :dev (do
             (log/info "Using Datomic in-memory storage (development mode)")
             (create-database cfg)
             (d/connect uri))

      :free (do
              (log/info "Using Datomic Free (localhost:4334)")
              (create-database cfg)
              (d/connect uri))

      :local (do
               (log/info "Using Datomic SQL persistence (H2 database)")
               (log/info "Data directory: ./data/datomic/")
               (create-database cfg)
               (d/connect uri))

      :pro (do
             (log/info "Using Datomic Pro")
             (create-database cfg)
             (d/connect uri))

      ;; Default
      (do
        (log/warn "Unknown server type:" server-type ", using in-memory")
        (create-database cfg)
        (d/connect uri)))))

(defn get-db
  "Get current database value from connection"
  [conn]
  (d/db conn))

(defn transact
  "Transaction helper with logging"
  [conn tx-data]
  (log/debug "Transacting:" (count tx-data) "entities")
  (let [result (d/transact conn tx-data)]
    (log/debug "Transaction completed, db:" (d/basis-t (d/db conn)))
    result))

(defn as-of
  "Get database as of specific transaction"
  [conn t]
  (d/as-of (d/db conn) t))

(defn since
  "Get database since specific transaction"
  [conn t]
  (d/since (d/db conn) t))

(defn history
  "Get history of database (all historical versions)"
  [conn]
  (d/history (d/db conn)))

(defn entity-as-of
  "Get entity as it was at specific point in time (time travel!)"
  [conn entity-id t]
  (-> conn
      (as-of t)
      (d/entity entity-id)
      (.touch)))

(defn entity-history
  "Get all historical versions of an entity"
  [conn entity-id]
  (->> (d/q '[:find ?attr ?value ?tx ?op
              :in $ ?e
              :where [?e ?attr ?value ?tx ?op]]
            (d/history (d/db conn))
            entity-id)
       (map (fn [[attr value tx op]]
              {:attr attr
               :value value
               :tx tx
               :op (if (= true op) :add :retract)}))
       (sort-by :tx)))

(comment
  ;; Development REPL usage
  (def cfg (datomic-blockchain.config/load-config))
  (def conn (connect cfg))
  (def db (get-db conn))
  @conn)
