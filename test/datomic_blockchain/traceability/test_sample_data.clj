(ns datomic-blockchain.traceability.test-sample-data
  "Tests for the labeled-path seeding seam.

  The complete chocolate path and each defect variant are seeded through
  one interface; the completeness assessment interface reads them back.
  Together these are the primitives issue 02 (labeled paths) and issue 03
  (score agrees with labels) build on."
  (:require [clojure.test :refer [deftest is testing]]
            [datomic.api :as d]
            [datomic-blockchain.datomic.schema :as schema]
            [datomic-blockchain.traceability.completeness :as completeness]
            [datomic-blockchain.traceability.sample-data :as sample])
  (:import [java.util UUID]))

(defn- approx=
  [expected actual]
  (< (Math/abs (- expected actual)) 1e-9))

(defn- fresh-conn
  []
  (let [uri (str "datomic:mem://sample-data-test-" (UUID/randomUUID))]
    (d/delete-database uri)
    (d/create-database uri)
    (let [conn (d/connect uri)]
      @(d/transact conn schema/full-schema)
      conn)))

(defn- assess
  [conn entity-id]
  (completeness/assess-completeness (d/db conn) entity-id))

;; =============================================================================
;; The base sample
;; =============================================================================

(deftest seed-uht-sample-returns-anchors-and-counts-test
  (testing "the dev-handler adapter's data: counts match the dataset,
            anchors point at the chocolate lot"
    (let [conn (fresh-conn)
          result (sample/seed-uht-sample! conn)
          anchors (:benchmark-anchors result)]
      (is (= {:agents 5 :products 4 :activities 7 :relationships 22}
             (:counts result)))
      (is (string? (:qr-code anchors)))
      (is (string? (:entity-id anchors)))
      (is (= :assessed (:status (assess conn (UUID/fromString (:entity-id anchors)))))))))

(deftest seeded-sample-is-queryable-by-qr-test
  (testing "the QR lookup finds the seeded chocolate entity"
    (let [conn (fresh-conn)
          anchors (-> (sample/seed-uht-sample! conn) :benchmark-anchors)
          db (d/db conn)
          found (d/q '[:find ?e .
                       :in $ ?qr
                       :where [?e :traceability/qr-code ?qr]]
                     db (:qr-code anchors))]
      (is (some? found)))))

;; =============================================================================
;; Labeled paths: one defect at a time
;; =============================================================================

(deftest labeled-paths-carry-their-defect-test
  (testing "each seeded defect is named by the assessment's missing evidence
            and scores below the complete path"
    (let [conn (fresh-conn)
          base (sample/seed-uht-sample! conn)
          complete (assess conn (UUID/fromString (get-in base [:benchmark-anchors :entity-id])))]
      (is (approx= 1.0 (:completeness-score complete))
          "the complete UHT chocolate path is the do-not-pull negative control")
      (doseq [defect (sample/labeled-path-defects)]
        (let [{:keys [anchors expected-missing-evidence]}
              (sample/seed-labeled-path! conn defect)
              result (assess conn (UUID/fromString (:entity-id anchors)))]
          (is (= :assessed (:status result)) (str defect))
          (is (some #{expected-missing-evidence} (:missing-evidence result))
              (str defect " is named in the missing evidence"))
          (is (< (:completeness-score result) (:completeness-score complete))
              (str defect " scores below the complete path")))))))

(deftest labeled-paths-coexist-with-base-sample-test
  (testing "variants are defect-scoped, so base + all four defects coexist
            and stay individually addressable by QR"
    (let [conn (fresh-conn)
          base-anchors (-> (sample/seed-uht-sample! conn) :benchmark-anchors)
          variant-anchors (mapv #(:anchors (sample/seed-labeled-path! conn %))
                                (sample/labeled-path-defects))
          all-qrs (conj (mapv :qr-code variant-anchors) (:qr-code base-anchors))
          db (d/db conn)]
      (is (= (count all-qrs) (count (distinct all-qrs))))
      (doseq [qr all-qrs]
        (is (some? (d/q '[:find ?e .
                          :in $ ?qr
                          :where [?e :traceability/qr-code ?qr]]
                        db qr))
            (str "QR " qr " resolves"))))))
