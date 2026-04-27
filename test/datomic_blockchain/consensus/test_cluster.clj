(ns datomic-blockchain.consensus.test-cluster
  (:require [clojure.test :refer :all]
            [datomic-blockchain.consensus.cluster :as cluster]
            [datomic-blockchain.cluster.member :as member]))

(defn- fake-cluster-member
  "Minimal ClusterMember implementation for consensus tests."
  ([node-id]
   (fake-cluster-member node-id 1))
  ([node-id quorum]
   (reify member/ClusterMember
     (node-id [_] node-id)
     (is-leader? [_] true)
     (leader-id [_] node-id)
     (members [_] {node-id "http://localhost:9999"})
     (quorum-size [_] quorum)
     (send-message [_ _ _ _] nil))))

(deftest execute-consensus-uses-proposal-id-test
  (let [proposal-id "proposal-test-1"
        tx-data {:prov/entity (random-uuid)
                 :prov/activity (random-uuid)
                 :prov/agent (random-uuid)}
        commit-called (atom false)
        result (cluster/execute-consensus! tx-data
                                           (fn [_]
                                             (reset! commit-called true)
                                             {:block-id "b1"})
                                           (fake-cluster-member "node-1")
                                           {:timeout-ms 1000
                                            :proposal-id proposal-id})]
    (is (= proposal-id (:proposal-id result)))
    (is (= :approved (:status result)))
    (is @commit-called)))

(deftest execute-consensus-rejects-invalid-tx-test
  (let [proposal-id "proposal-test-2"
        commit-called (atom false)
        result (cluster/execute-consensus! {}
                                           (fn [_]
                                             (reset! commit-called true)
                                             {:block-id "b1"})
                                           (fake-cluster-member "node-1")
                                           {:timeout-ms 1000
                                            :proposal-id proposal-id})]
    (is (= proposal-id (:proposal-id result)))
    (is (= :rejected (:status result)))
    (is (false? @commit-called))))
