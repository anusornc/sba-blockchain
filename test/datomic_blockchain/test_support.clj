(ns datomic-blockchain.test-support
  "One seam for provisioning fresh in-memory Datomic databases in tests.

   Hides URI minting, database creation, schema install, and teardown so
   provisioning changes happen here alone. Two styles:

   - wrap a test body: (with-fresh-db [conn] ...) — releases and deletes
     afterwards, even on failure;
   - lease style for helper-shaped suites: (fresh-conn) then (retire! conn),
     or a suite-level (use-fixtures :each (fn [f] (f) (retire-all!))) sweep.

   Suites must not hand-roll any of this: leaked mem databases accumulate
   across a run and only release+delete reclaims them. The lease registry
   assumes sequential test execution (kaocha's default)."
  (:require [datomic.api :as d]
            [datomic-blockchain.datomic.schema :as schema])
  (:import [java.util UUID]))

(def ^:private leases
  "Active leases: conn identity -> {:uri :conn}."
  (atom {}))

(defn- create-db!
  []
  (let [uri (str "datomic:mem://test-" (UUID/randomUUID))]
    (d/create-database uri)
    (let [conn (d/connect uri)]
      @(d/transact conn schema/full-schema)
      {:uri uri :conn conn})))

(defn- dispose!
  [{:keys [conn uri]}]
  (try (d/release conn) (catch Exception _))
  (try (d/delete-database uri) (catch Exception _)))

(defn fresh-conn
  "A new in-memory connection with the full schema installed. Retire it
   with (retire! conn) when the test is done."
  []
  (let [{:keys [conn] :as db} (create-db!)]
    (swap! leases assoc (identity conn) db)
    conn))

(defn retire!
  "Release the connection and delete its database. Idempotent."
  [conn]
  (when-let [db (get @leases (identity conn))]
    (dispose! db)
    (swap! leases dissoc (identity conn)))
  nil)

(defn retire-all!
  "Release and delete every outstanding lease. Use as a suite-level
   sweep when tests obtain connections through helpers."
  []
  (doseq [db (vals @leases)]
    (dispose! db))
  (reset! leases {})
  nil)

(defmacro with-fresh-db
  "Run body with conn bound to a fresh schema-installed in-memory
   database; always releases the connection and deletes the database."
  [[conn] & body]
  `(let [conn# (fresh-conn)]
     (try
       (let [~conn conn#]
         ~@body)
       (finally
         (retire! conn#)))))
