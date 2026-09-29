(ns datomic-blockchain.test-system
  "System-level regression tests for the Integrant wiring (ADR-0001)."
  (:require [clojure.test :refer [deftest is testing]]
            [integrant.core :as ig]
            [datomic-blockchain.system :as system]))

(defn- local-port
  "The port the Jetty server is actually bound to."
  [jetty]
  (-> jetty .getConnectors (aget 0) .getLocalPort))

(deftest system-binds-configured-port-test
  (testing "the Integrant :http/server binds the configured port — the port
            used to land in the config argument slot, silently binding 3000"
    (let [test-port 3999
          sys (ig/init (-> (system/system-config :dev {:port test-port})
                           (assoc :datomic/config
                                  {:datomic {:server-type :dev
                                             :db-name "port-binding-regression"}})))]
      (try
        (let [{:keys [server port]} (:http/server sys)]
          (is (some? server) "system returns a jetty server")
          (is (.isStarted server))
          (is (= test-port port) "server map reports the configured port")
          (is (= test-port (local-port server))
              "jetty actually bound the configured port"))
        (finally
          (system/stop sys))))))
