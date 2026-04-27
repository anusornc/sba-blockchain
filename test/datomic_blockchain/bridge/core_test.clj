(ns datomic-blockchain.bridge.core-test
  (:require [clojure.test :refer [deftest is testing]]
            [datomic.api :as d]
            [datomic-blockchain.bridge.core :as bridge]
            [datomic-blockchain.bridge.merkle :as merkle]
            [datomic-blockchain.datomic.schema :as schema]))

(def entity-id #uuid "550e8400-e29b-41d4-a716-446655440000")
(def activity-id #uuid "550e8400-e29b-41d4-a716-446655440001")
(def agent-id #uuid "550e8400-e29b-41d4-a716-446655440002")

(def valid-payload
  {:prov/entity {:prov/entity entity-id
                 :prov/entity-type :product/uht-milk
                 :traceability/batch "UHT-CHOC-CM-2024-001"}
   :prov/activity {:prov/activity activity-id
                   :prov/activity-type :activity/processing}
   :prov/agent {:prov/agent agent-id
                :prov/agent-type :organization/manufacturer}
   :prov/relationships {:prov/used entity-id
                        :prov/wasGeneratedBy activity-id
                        :prov/wasAssociatedWith agent-id}})

(defn- mem-conn
  [db-name]
  (let [uri (str "datomic:mem://" db-name)]
    (d/delete-database uri)
    (d/create-database uri)
    (let [conn (d/connect uri)]
      @(d/transact conn schema/full-schema)
      conn)))

(deftest initiate-transfer-builds-payload-from-provenance
  (let [conn (mem-conn "bridge-initiate-transfer")]
    (try
      @(d/transact
        conn
        [{:prov/entity entity-id
          :prov/entity-type :product/uht-milk
          :prov/wasGeneratedBy activity-id
          :traceability/batch "UHT-CHOC-CM-2024-001"
          :traceability/product entity-id}
         {:prov/activity activity-id
          :prov/activity-type :activity/processing
          :prov/startedAtTime #inst "2024-01-15T08:30:00.000-00:00"
          :prov/wasAssociatedWith [agent-id]}
         {:prov/agent agent-id
          :prov/agent-type :organization/manufacturer
          :prov/agent-name "Northern Thai UHT Processing Ltd."}])
      (let [result (bridge/initiate-transfer
                    conn
                    {:source-chain "farm-chain"
                     :target-chain "factory-chain"
                     :validators ["v1" "v2" "v3"]
                     :threshold 2}
                    entity-id)
            payload (get-in result [:transaction :payload])]
        (is (:success result))
        (is (bridge/validate-prov-o-payload payload))
        (is (= (merkle/hash-tx payload)
               (get-in result [:transaction :merkle-tx-hash])))
        (is (merkle/verify-proof
             (get-in result [:transaction :merkle-tx-hash])
             (get-in result [:transaction :merkle-proof])
             (get-in result [:transaction :merkle-root])))
        (is (= "UHT-CHOC-CM-2024-001"
               (get-in payload [:prov/entity :traceability/batch]))))
      (finally
        (d/delete-database "datomic:mem://bridge-initiate-transfer")))))

(deftest execute-transfer-persists-bridged-entity
  (let [conn (mem-conn "bridge-execute-transfer")
        tx {:tx-id "source-tx-1"
            :source-chain "farm-chain"
            :target-chain "factory-chain"
            :signatures ["sig-1" "sig-2"]
            :payload valid-payload}]
    (try
      (let [result (bridge/execute-transfer
                    conn
                    {:source-chain "farm-chain"
                     :target-chain "factory-chain"
                     :validators ["v1" "v2" "v3"]
                     :threshold 2}
                    tx)
            db (d/db conn)
            bridged (d/entity db [:blockchain/cross-chain-tx "source-tx-1"])]
        (is (= {:success true :status :executed} result))
        (is (= "farm-chain" (:blockchain/cross-chain-source bridged)))
        (is (= "UHT-CHOC-CM-2024-001" (:traceability/batch bridged))))
      (finally
        (d/delete-database "datomic:mem://bridge-execute-transfer")))))

(deftest execute-transfer-rejects-invalid-or-under-signed-input
  (let [conn (mem-conn "bridge-execute-transfer-invalid")
        bridge-config {:source-chain "farm-chain"
                       :target-chain "factory-chain"
                       :validators ["v1" "v2" "v3"]
                       :threshold 2}]
    (try
      (testing "under-signed transfer remains pending"
        (is (= {:success false :status :pending :needed 1}
               (bridge/execute-transfer
                conn
                bridge-config
                {:tx-id "source-tx-2"
                 :source-chain "farm-chain"
                 :signatures ["sig-1"]
                 :payload valid-payload}))))
      (testing "invalid payload fails without Datomic write"
        (is (= {:success false :status :failed :reason "Invalid PROV-O payload"}
               (bridge/execute-transfer
                conn
                bridge-config
                {:tx-id "source-tx-3"
                 :source-chain "farm-chain"
                 :signatures ["sig-1" "sig-2"]
                 :payload {:prov/entity {}}}))))
      (finally
        (d/delete-database "datomic:mem://bridge-execute-transfer-invalid")))))

(deftest validate-prov-o-payload-rejects-inconsistent-relationships
  (testing "activity and agent relationships must match the enclosed PROV records"
    (is (false? (bridge/validate-prov-o-payload
                 (assoc-in valid-payload
                           [:prov/relationships :prov/wasGeneratedBy]
                           #uuid "650e8400-e29b-41d4-a716-446655440001"))))
    (is (false? (bridge/validate-prov-o-payload
                 (assoc-in valid-payload
                           [:prov/relationships :prov/wasAssociatedWith]
                           #uuid "650e8400-e29b-41d4-a716-446655440002")))))
  (testing "complete entity/activity/agent records are required"
    (is (false? (bridge/validate-prov-o-payload
                 (dissoc valid-payload :prov/activity))))
    (is (false? (bridge/validate-prov-o-payload
                 (assoc valid-payload :prov/agent {}))))))

(deftest execute-transfer-rejects-invalid-merkle-proof
  (let [conn (mem-conn "bridge-execute-transfer-invalid-merkle")
        bridge-config {:source-chain "farm-chain"
                       :target-chain "factory-chain"
                       :validators ["v1" "v2" "v3"]
                       :threshold 2}
        source-txs [valid-payload
                    (assoc-in valid-payload [:prov/entity :traceability/batch] "OTHER")]
        tree (merkle/build-merkle-tree source-txs)
        tx-hash (merkle/hash-tx valid-payload)
        proof (merkle/generate-proof tree tx-hash)
        tampered-proof (assoc-in proof [:proof-path 0 :hash] (merkle/sha-256 "tampered"))]
    (try
      (is (= {:success false :status :failed :reason "Invalid Merkle proof"}
             (bridge/execute-transfer
              conn
              bridge-config
              {:tx-id "source-tx-4"
               :source-chain "farm-chain"
               :signatures ["sig-1" "sig-2"]
               :payload valid-payload
               :merkle-tx-hash tx-hash
               :merkle-root (:root tree)
               :merkle-proof tampered-proof})))
      (is (nil? (d/entity (d/db conn) [:blockchain/cross-chain-tx "source-tx-4"])))
      (finally
        (d/delete-database "datomic:mem://bridge-execute-transfer-invalid-merkle")))))

(deftest execute-transfer-accepts-valid-payload-bound-merkle-proof
  (let [conn (mem-conn "bridge-execute-transfer-valid-merkle")
        bridge-config {:source-chain "farm-chain"
                       :target-chain "factory-chain"
                       :validators ["v1" "v2" "v3"]
                       :threshold 2}
        tree (merkle/build-merkle-tree [valid-payload])
        tx-hash (merkle/hash-tx valid-payload)
        proof (merkle/generate-proof tree tx-hash)]
    (try
      (is (= {:success true :status :executed}
             (bridge/execute-transfer
              conn
              bridge-config
              {:tx-id "source-tx-5"
               :source-chain "farm-chain"
               :signatures ["sig-1" "sig-2"]
               :payload valid-payload
               :merkle-tx-hash tx-hash
               :merkle-root (:root tree)
               :merkle-proof (:proof-path proof)})))
      (is (some? (d/entity (d/db conn) [:blockchain/cross-chain-tx "source-tx-5"])))
      (finally
        (d/delete-database "datomic:mem://bridge-execute-transfer-valid-merkle")))))
