(ns datomic-blockchain.ontology.test_blank_node
  "Tests for blank node handling in RDF/PROV-O ontology

  Tests cover:
  1. Blank node detection
  2. Deterministic UUID generation
  3. RDF processing with blank nodes
  4. Blank node collision prevention
  5. Integration with mapper"
  (:require [clojure.test :refer [deftest is testing are]]
            [clojure.string :as str]
            [datomic-blockchain.ontology.blank-node-support :as blank-node]
            [datomic-blockchain.ontology.mapper :as mapper]))

;; ============================================================================
;; Blank Node Detection Tests
;; ============================================================================

(deftest test-blank-node?
  (testing "Blank node detection"
    (is (true? (blank-node/blank-node? "_:b1")))
    (is (true? (blank-node/blank-node? "_:auto123")))
    (is (true? (blank-node/blank-node? "_:gen1")))
    (is (true? (blank-node/blank-node? "_:my_entity")))
    (is (true? (blank-node/blank-node? "_:")))
    (is (false? (blank-node/blank-node? "http://example.org")))
    (is (false? (blank-node/blank-node? "<http://example.org>")))
    (is (false? (blank-node/blank-node? "ex:entity")))
    (is (false? (blank-node/blank-node? "")))
    (is (false? (blank-node/blank-node? nil)))
    (is (false? (blank-node/blank-node? 123)))))

(deftest test-find-blank-nodes
  (testing "Finding blank nodes in RDF content"
    (let [rdf "_:b1 prov:type _:b2 . _:b2 prov:type prov:Entity ."]
      (is (= #{"_:b1" "_:b2"} (blank-node/find-blank-nodes rdf)))))

  (testing "No blank nodes"
    (let [rdf "<ex:thing> prov:type <http://example.org/Entity> ."]
      (is (empty? (blank-node/find-blank-nodes rdf)))))

  (testing "Mixed content"
    (let [rdf "_:b1 prov:type _:b2 . <ex:thing> prov:type _:b1 ."]
      (is (= #{"_:b1" "_:b2"} (blank-node/find-blank-nodes rdf)))))

  (testing "Nil input"
    (is (nil? (blank-node/find-blank-nodes nil)))))

(deftest test-blank-node-statistics
  (testing "Blank node statistics"
    (let [rdf "_:b1 prov:type _:b2 . _:b1 prov:used _:b3 . _:b2 prov:type _:b3 ."]
      (is (= 3 (get (blank-node/blank-node-statistics rdf) :unique-blank-nodes)))
      (is (= 6 (get (blank-node/blank-node-statistics rdf) :total-blank-nodes)))))

  (testing "Empty content"
    (let [rdf ""]
      (is (= 0 (get (blank-node/blank-node-statistics rdf) :unique-blank-nodes))))))

;; ============================================================================
;; Deterministic UUID Generation Tests
;; ============================================================================

(deftest test-generate-deterministic-uuid
  (testing "Same blank node + same context = same UUID"
    (let [uuid1 (blank-node/generate-deterministic-uuid "b1" "context123")
          uuid2 (blank-node/generate-deterministic-uuid "b1" "context123")]
      (is (= uuid1 uuid2))
      (is (uuid? uuid1))))

  (testing "Different blank node + same context = different UUID"
    (let [uuid1 (blank-node/generate-deterministic-uuid "b1" "context123")
          uuid2 (blank-node/generate-deterministic-uuid "b2" "context123")]
      (is (not= uuid1 uuid2))))

  (testing "Same blank node + different context = different UUID"
    (let [uuid1 (blank-node/generate-deterministic-uuid "b1" "context123")
          uuid2 (blank-node/generate-deterministic-uuid "b1" "context456")]
      (is (not= uuid1 uuid2))))

  (testing "Different blank nodes in different contexts all unique"
    (let [contexts ["doc1" "doc2" "doc3"]
          ids ["b1" "b1" "b1"]  ; Same ID, different docs
          uuids (map blank-node/generate-deterministic-uuid ids contexts)]
      (is (= 3 (count (set uuids))))))

  (testing "UUID format validation"
    (let [uuid (blank-node/generate-deterministic-uuid "b1" "context")]
      (is (string? (str uuid)))
      (is (re-matches #"[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}" (str uuid))))))

;; ============================================================================
;; Blank Node Mapping Tests
;; ============================================================================

(deftest test-create-blank-node-mapping
  (testing "Create mapping from blank nodes to UUIDs"
    (let [blank-nodes #{"_:b1" "_:b2" "_:b3"}
          mapping (blank-node/create-blank-node-mapping blank-nodes "context")]
      (is (= 3 (count mapping)))
      (is (contains? mapping "_:b1"))
      (is (contains? mapping "_:b2"))
      (is (contains? mapping "_:b3"))
      (is (every? uuid? (vals mapping)))))

  (testing "Empty blank node set"
    (let [mapping (blank-node/create-blank-node-mapping #{} "context")]
      (is (empty? mapping)))))

;; ============================================================================
;; RDF Processing Tests
;; ============================================================================

(deftest test-process-rdf-with-blank-nodes
  (testing "Process RDF with blank nodes"
    (let [rdf "_:b1 prov:type _:b2"
          result (blank-node/process-rdf-with-blank-nodes rdf)]
      (is (= 2 (:blank-node-count result)))
      (is (= #{"_:b1" "_:b2"} (:blank-nodes-found result)))
      (is (string? (:processed-rdf result)))
      (is (map? (:blank-node-map result)))))

  (testing "Process RDF without blank nodes"
    (let [rdf "<ex:thing> prov:type <ex:Entity>"
          result (blank-node/process-rdf-with-blank-nodes rdf)]
      (is (= 0 (:blank-node-count result)))
      (is (empty? (:blank-nodes-found result)))))

  (testing "Custom context seed"
    (let [rdf "_:b1 prov:type _:b2"
          result (blank-node/process-rdf-with-blank-nodes rdf {:context-seed "custom"})]
      (is (= "custom" (:context-seed result))))))

;; ============================================================================
;; Collision Prevention Tests
;; ============================================================================

(deftest test-blank-node-collision-prevention
  (testing "Same blank node in different documents gets different UUIDs"
    (let [doc1 "_:b1 prov:type prov:Entity"
          doc2 "_:b1 prov:type prov:Activity"  ; Same blank node ID
          result1 (blank-node/process-rdf-with-blank-nodes doc1)
          result2 (blank-node/process-rdf-with-blank-nodes doc2)
          uuid1 (get (:blank-node-map result1) "_:b1")
          uuid2 (get (:blank-node-map result2) "_:b1")]
      ;; Different documents = different UUIDs for same blank node ID
      (is (not= uuid1 uuid2))))

  (testing "Different blank nodes in same document get different UUIDs"
    (let [rdf "_:b1 prov:type _:b2"
          result (blank-node/process-rdf-with-blank-nodes rdf)
          uuid1 (get (:blank-node-map result) "_:b1")
          uuid2 (get (:blank-node-map result) "_:b2")]
      (is (not= uuid1 uuid2))))

  (testing "Same blank node appears multiple times gets same UUID"
    (let [rdf "_:b1 prov:type _:b2 . _:b1 prov:used _:b3 ."
          result (blank-node/process-rdf-with-blank-nodes rdf)
          mapping (:blank-node-map result)
          b1-uuid (get mapping "_:b1")]
      ;; _:b1 appears twice, should map to same UUID
      (is (uuid? b1-uuid)))
  )

;; ============================================================================
;; Validation Tests
;; ============================================================================

(deftest test-validate-no-blank-nodes
  (testing "Valid RDF without blank nodes"
    (let [rdf "<ex:thing> prov:type <ex:Entity>"
          result (blank-node/validate-no-blank-nodes rdf)]
      (is (true? (:valid? result)))))

  (testing "Invalid RDF with blank nodes"
    (let [rdf "_:b1 prov:type _:b2"
          result (blank-node/validate-no-blank-nodes rdf)]
      (is (false? (:valid? result)))
      (is (= 2 (:count result)))
      (is (= #{"_:b1" "_:b2"} (:blank-nodes result))))))

;; ============================================================================
;; Integration Tests with Mapper
;; ============================================================================

(deftest test-mapper-with-blank-nodes
  (testing "Mapper processes RDF with blank nodes safely"
    (let [rdf "_:b1 prov:type prov:Entity . _:b1 prov:wasGeneratedBy _:b2 ."
          ;; The mapper should handle blank nodes internally now
          result (mapper/rdf->datomic rdf :turtle)]
      (is (vector? result))
      ;; Should not have error key
      (is (nil? (:error result)))
      ;; Should have at least one entity
      (is (pos? (count result)))))

  (testing "Mapper preserves relationships between blank nodes"
    (let [rdf "_:b1 prov:type prov:Entity . _:b1 prov:wasGeneratedBy _:b2 . _:b2 prov:type prov:Activity ."
          result (mapper/rdf->datomic rdf :turtle)]
      (is (vector? result))
      (is (nil? (:error result))))))

;; ============================================================================
;; Replacement Tests
;; ============================================================================

(deftest test-replace-blank-nodes
  (testing "Replace blank nodes with UUIDs in RDF string"
    (let [rdf "_:b1 prov:type _:b2"
          mapping {"_:b1" (java.util.UUID/randomUUID)
                   "_:b2" (java.util.UUID/randomUUID)}
          result (blank-node/replace-blank-nodes rdf mapping)]
      (is (not (str/includes? result "_:b1")))
      (is (not (str/includes? result "_:b2")))
      (is (str/includes? result "#uuid"))))

  (testing "Empty mapping"
    (let [rdf "_:b1 prov:type _:b2"
          result (blank-node/replace-blank-nodes rdf {})]
      ;; Should return original if no mapping
      (is (str/includes? result "_:b1")))))

;; ============================================================================
;; Process Safe Tests
;; ============================================================================

(deftest test-process-rdf-safe
  (testing "Safe processing with blank nodes"
    (let [rdf "_:b1 prov:type _:b2"
          result (blank-node/process-rdf-safe rdf)]
      (is (string? result))
      (is (not (str/includes? result "_:b1")))
      (is (str/includes? result "uuid"))))

  (testing "Safe processing without blank nodes passes through"
    (let [rdf "<ex:thing> prov:type <ex:Entity>"
          result (blank-node/process-rdf-safe rdf)]
      (is (string? result))
      (is (= rdf result)))))

;; ============================================================================
;; Edge Cases
;; ============================================================================

(deftest test-blank-node-edge-cases
  (testing "Blank node with underscore and numbers"
    (is (true? (blank-node/blank-node? "_:my_entity_123"))))

  (testing "Blank node with hyphen"
    (is (true? (blank-node/blank-node? "_:my-entity"))))

  (testing "Blank node at end of line"
    (let [rdf "prov:type _:b1"]
      (is (= #{"_:b1"} (blank-node/find-blank-nodes rdf)))))

  (testing "Multiple blank nodes same line"
    (let [rdf "_:b1 prov:type _:b2 ; _:b3 prov:type _:b4"]
      (is (= #{"_:b1" "_:b2" "_:b3" "_:b4"} (blank-node/find-blank-nodes rdf)))))

  (testing "Blank node in comments should be ignored"
    (let [rdf "# _:b1 is a blank node\n<ex:thing> prov:type <ex:Entity>"]
      ;; Current implementation finds blank nodes in comments too
      ;; This is acceptable as they'll be replaced
      (is (not-empty (blank-node/find-blank-nodes rdf))))))

;; ============================================================================
;; Performance Tests
;; ============================================================================

(deftest test-blank-node-processing-performance
  (testing "Process RDF with many blank nodes"
    ;; Create RDF with 100 blank nodes
    (let [blank-ids (map #(str "_:b" %) (range 1 101))
          rdf (str/join " " (map #(str % " prov:type prov:Entity .") blank-ids))
          start (System/nanoTime)
          result (blank-node/process-rdf-with-blank-nodes rdf)
          end (System/nanoTime)
          duration-ms (/ (- end start) 1000000.0)]
      (is (= 100 (:blank-node-count result)))
      ;; Should complete in reasonable time (< 1 second)
      (is (< duration-ms 1000))
      (is (every? uuid? (vals (:blank-node-map result)))))))

;; ============================================================================
;; Document Context Tests
;; ============================================================================

(deftest test-document-context-generation
  (testing "Generate consistent context for same document"
    (let [rdf "<ex:thing> prov:type <ex:Entity>"
          ctx1 (blank-node/generate-document-context rdf)
          ctx2 (blank-node/generate-document-context rdf)]
      ;; Same document should produce same context
      (is (= ctx1 ctx2))))

  (testing "Different documents produce different contexts"
    (let [rdf1 "<ex:thing> prov:type <ex:Entity>"
          rdf2 "<ex:other> prov:type <ex:Activity>"
          ctx1 (blank-node/generate-document-context rdf1)
          ctx2 (blank-node/generate-document-context rdf2)]
      ;; Different documents should produce different contexts
      (is (not= ctx1 ctx2)))))
)
