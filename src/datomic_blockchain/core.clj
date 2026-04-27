(ns datomic-blockchain.core
  "Main entry point for the Datomic-based Semantic Traceability System."
  (:gen-class)
  (:require [datomic-blockchain.config :as config]
            [datomic-blockchain.datomic.connection :as conn]
            [datomic-blockchain.datomic.schema :as schema]
            [datomic-blockchain.api.routes :as api]
            [datomic-blockchain.permission.policy :as policy]
            [taoensso.timbre :as log]
            [clojure.pprint :as pp]
            [datomic-blockchain.cluster.member :as cluster]))

(defn -main
  "Main entry point"
  [& args]
  (log/info "Starting Datomic Semantic Traceability System...")
  ;; Check for profile override from environment
  (let [profile-env (System/getenv "DATOMIC_PROFILE")
        profile (if profile-env 
                  (keyword profile-env)
                  :dev)]
    (log/info "Loading configuration (profile:" profile ")...")
    (let [cfg (config/load-config profile)]
      (log/info "Configuration loaded for profile:" profile)

    ;; Check if cluster mode is enabled
    (log/info "Cluster mode:" (Boolean/parseBoolean (System/getenv "CLUSTER_MODE")))

    (log/info "Connecting to Datomic...")
    (let [db-connection (conn/connect cfg)]
      (log/info "Connected to Datomic")
      (log/info "Initializing schema...")
      (schema/install-schema db-connection)
      (log/info "Schema initialized")

      ;; Initialize cluster if enabled
      (when (cluster/cluster-enabled?)
        (log/info "Initializing cluster mode...")
        (cluster/initialize-cluster!)
        (log/info "Cluster initialized"))

      (log/info "Initializing permission policy store...")
      (let [policy-store (policy/init-policy-store :default)]
        (log/info "Permission policy store initialized.")

        (log/info "Starting API server...")
        (let [port (get-in cfg [:server :port] 3000)
              server-map (api/start-server db-connection policy-store cfg port)]
          (log/info "API server started on port" port)
          (log/info "System ready!")

          ;; Print startup banner
          (let [server-type (get-in cfg [:datomic :server-type])
                storage-desc (case server-type
                              :dev "In-Memory (Development)"
                              :local "Persistent (H2 Database)"
                              :free "Datomic Free"
                              :pro "Datomic Pro"
                              "Unknown")]
            (println "\n========================================")
            (println "  Datomic Semantic Traceability Server")
            (println "  Running on http://localhost:" port)
            (println "  Storage:" storage-desc)

            (when (cluster/cluster-enabled?)
              (let [cluster (cluster/get-cluster)]
                (println "  Cluster Mode: ENABLED")
                (println "  Node ID:" (cluster/node-id cluster))
                (println "  Is Leader:" (cluster/is-leader? cluster))
                (println "  Cluster Members:" (keys (cluster/members cluster)))))

            (println "========================================\n"))

          ;; Keep the JVM alive and monitor server
          (log/info "Server running, monitoring...")
          (let [jetty-server (:server server-map)]
            ;; This thread keeps the JVM from exiting
            (while (and jetty-server (.isStarted jetty-server))
              (Thread/sleep 60000)
              (log/debug "Server heartbeat")))))))))

(defn start-dev
  "Start the system in development mode"
  []
  (log/set-level! :debug)
  (-main))
