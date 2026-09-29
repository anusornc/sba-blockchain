(ns datomic-blockchain.core
  "Main entry point for the Datomic-based Semantic Traceability System.
  Thin delegate to the Integrant system (ADR-0001); kept so documented
  run commands (`clj -M -m datomic-blockchain.core`) stay valid."
  (:gen-class)
  (:require [datomic-blockchain.system :as system]))

(defn -main
  "Start the system via the single Integrant wiring.

  Profile comes from DATOMIC_PROFILE (default :dev), matching the
  historical behaviour of this entry point; system/-main otherwise
  reads PROFILE/PORT itself."
  [& _args]
  (let [profile (some-> (or (System/getenv "DATOMIC_PROFILE") "dev")
                        keyword)]
    (system/-main profile)))

(defn start-dev
  "Start the system in development mode"
  []
  (system/start-dev))
