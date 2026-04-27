(ns datomic-blockchain.system
  "System component management using Integrant
   
   Provides a declarative, data-driven approach to system composition.
   Replaces dynamic binding with explicit dependency injection.
   
   Usage:
     (require '[datomic-blockchain.system :as system])
     
     ;; Start the system
     (def system (system/start))
     
     ;; Access components
     (def conn (:datomic/connection system))
     (def server (:http/server system))
     
     ;; Stop the system
     (system/stop system)
   "
  (:require [integrant.core :as ig]
            [taoensso.timbre :as log]
            [datomic-blockchain.config :as config]
            [datomic-blockchain.datomic.connection :as conn]
            [datomic-blockchain.datomic.schema :as schema]
            [datomic-blockchain.permission.policy :as policy]
            [datomic-blockchain.api.routes :as api]
            [datomic-blockchain.cluster.member :as cluster]))

;; ============================================================================
;; Integrant Configuration
;; ============================================================================

(def ^:private base-config
  "Base system configuration
   
   This defines the system topology - which components exist and their
   dependencies. Each key becomes a component in the running system.
   
   The config uses Integrant's keyword-based component references:
   #ig/ref :component/key means 'inject the value of :component/key here'"
  {:datomic/config {}
   :datomic/connection {:config (ig/ref :datomic/config)}
   :datomic/schema {:connection (ig/ref :datomic/connection)}
   :permission/policy-store {:strategy :default}
   :cluster/member {:config (ig/ref :datomic/config)}
   :http/server {:connection (ig/ref :datomic/connection)
                 :policy-store (ig/ref :permission/policy-store)
                 :port 3000}})

(defn system-config
  "Generate system configuration for a given profile.
   
   Options:
     :profile - :dev, :test, :prod (default: :dev)
     :port    - HTTP server port (overrides config file)"
  ([] (system-config :dev))
  ([profile] (system-config profile {}))
  ([profile {:keys [port]}]
   (let [app-config (config/load-config profile)
         http-port (or port (get-in app-config [:server :port]) 3000)]
     (assoc-in base-config [:http/server :port] http-port))))

;; ============================================================================
;; Component Initialization Methods
;; ============================================================================

;; Datomic Configuration
(defmethod ig/init-key :datomic/config
  [_ _]
  (log/info "Initializing Datomic configuration...")
  (config/load-config))

;; Datomic Connection
(defmethod ig/init-key :datomic/connection
  [_ {:keys [config]}]
  (log/info "Connecting to Datomic...")
  (let [connection (conn/connect config)]
    (log/info "Datomic connected")
    connection))

(defmethod ig/halt-key! :datomic/connection
  [_ connection]
  (log/info "Disconnecting from Datomic...")
  ;; Datomic connections don't need explicit cleanup
  (log/info "Datomic disconnected"))

;; Schema Installation
(defmethod ig/init-key :datomic/schema
  [_ {:keys [connection]}]
  (log/info "Installing schema...")
  (schema/install-schema connection)
  (log/info "Schema installed")
  {:installed true :timestamp (java.util.Date.)})

;; Permission Policy Store
(defmethod ig/init-key :permission/policy-store
  [_ {:keys [strategy]}]
  (log/info "Initializing permission policy store...")
  (policy/init-policy-store (or strategy :default)))

;; Cluster Member
(defmethod ig/init-key :cluster/member
  [_ {:keys [config]}]
  (when (cluster/cluster-enabled?)
    (log/info "Initializing cluster member...")
    (cluster/initialize-cluster!)
    (cluster/get-cluster)))

(defmethod ig/halt-key! :cluster/member
  [_ member]
  (when member
    (log/info "Shutting down cluster member...")
    ;; Add any cluster cleanup here
    (log/info "Cluster member shut down")))

;; HTTP Server
(defmethod ig/init-key :http/server
  [_ {:keys [connection policy-store port]}]
  (log/info "Starting HTTP server on port" port "...")
  (let [server-map (api/start-server connection policy-store port)]
    (log/info "HTTP server started on port" port)
    server-map))

(defmethod ig/halt-key! :http/server
  [_ server-map]
  (log/info "Stopping HTTP server...")
  (when-let [jetty-server (:server server-map)]
    (.stop jetty-server))
  (log/info "HTTP server stopped"))

;; ============================================================================
;; System Lifecycle
;; ============================================================================

(defn start
  "Start the system with the given configuration.
   
   Returns a system map containing all initialized components.
   The system map is an immutable value - use it to access components
   and pass it to stop when shutting down.
   
   Example:
     (def system (system/start :dev {:port 8080}))
     (def conn (:datomic/connection system))"
  ([] (start :dev {}))
  ([profile] (start profile {}))
  ([profile opts]
   (let [config (system-config profile opts)]
     (log/info "Starting system with profile:" profile)
     (ig/init config))))

(defn stop
  "Stop a running system.
   
   Halts all components in reverse dependency order.
   Always call this on shutdown to ensure clean resource cleanup.
   
   Example:
     (system/stop system)"
  [system]
  (log/info "Stopping system...")
  (ig/halt! system)
  (log/info "System stopped"))

(defn restart
  "Restart a running system with a new configuration.
   
   Stops the current system and starts a new one.
   Returns the new system map.
   
   Example:
     (def system (system/restart system :prod))"
  ([system] (restart system :dev {}))
  ([system profile] (restart system profile {}))
  ([system profile opts]
   (stop system)
   (start profile opts)))

;; ============================================================================
;; Component Access Helpers
;; ============================================================================

(defn connection
  "Get the Datomic connection from a running system."
  [system]
  (:datomic/connection system))

(defn policy-store
  "Get the permission policy store from a running system."
  [system]
  (:permission/policy-store system))

(defn server
  "Get the HTTP server from a running system."
  [system]
  (:http/server system))

(defn cluster-member
  "Get the cluster member from a running system (if enabled)."
  [system]
  (:cluster/member system))

;; ============================================================================
;; Development Helpers
;; ============================================================================

(defonce ^:private dev-system (atom nil))

(defn start-dev
  "Start a development system and store it in the dev-system atom.
   
   This is the recommended way to start the system during development.
   The system is stored globally for easy access in the REPL.
   
   Example:
     (system/start-dev)
     (def conn (system/dev-connection))"
  ([] (start-dev {}))
  ([opts]
   (let [sys (start :dev opts)]
     (reset! dev-system sys)
     (println "\n========================================")
     (println "  Development System Started")
     (println "  Profile: :dev")
     (when-let [port (get-in sys [:http/server :port])]
       (println "  Server: http://localhost:" port))
     (println "========================================\n")
     sys)))

(defn stop-dev
  "Stop the development system."
  []
  (when-let [sys @dev-system]
    (stop sys)
    (reset! dev-system nil)
    (println "Development system stopped")))

(defn get-dev-system
  "Get the current development system (if running)."
  []
  @dev-system)

(defn dev-connection
  "Get the Datomic connection from the dev system."
  []
  (when-let [sys @dev-system]
    (connection sys)))

(defn dev-policy-store
  "Get the policy store from the dev system."
  []
  (when-let [sys @dev-system]
    (policy-store sys)))

(defn restart-dev
  "Restart the development system."
  ([] (restart-dev {}))
  ([opts]
   (stop-dev)
   (start-dev opts)))

;; ============================================================================
;; Production Entry Point
;; ============================================================================

(defn -main
  "Production entry point using Integrant system.
   
   This is an alternative to core.clj that uses the component system.
   Eventually, core.clj should be refactored to use this.
   
   Usage:
     clj -M -m datomic-blockchain.system"
  [& args]
  (let [profile (keyword (or (System/getenv "PROFILE") "prod"))
        port (some-> (System/getenv "PORT") Integer/parseInt)
        system (start profile (when port {:port port}))]
    
    ;; Add shutdown hook
    (.addShutdownHook
     (Runtime/getRuntime)
     (Thread. #(do
                 (log/info "Shutdown signal received")
                 (stop system))))
    
    ;; Keep the main thread alive
    (let [server (:http/server system)
          jetty-server (:server server)]
      (while (and jetty-server (.isStarted jetty-server))
        (Thread/sleep 60000)
        (log/debug "System heartbeat")))))

(comment
  ;; Development workflow example
  
  ;; 1. Start the system
  (start-dev)
  
  ;; 2. Access components
  (def conn (dev-connection))
  (def store (dev-policy-store))
  
  ;; 3. Use components
  (require '[datomic.api :as d])
  (d/q '[:find ?e :where [?e :prov/entity]] (d/db conn))
  
  ;; 4. Restart after code changes
  (restart-dev)
  
  ;; 5. Clean shutdown
  (stop-dev)
  
  ;; Production system example
  (def prod-system (start :prod))
  (stop prod-system)
  
  ;; Custom port
  (start-dev {:port 8080})
  
  ;; Inspect system
  (keys (dev-system))
  (:datomic/config (dev-system))
  )
