(ns datomic-blockchain.traceability.ui-contract
  "The Trace View contract: the versioned, read-only projection of a
   batch's Journey served to the traceability frontend (ADR-0002).

   Reads the Deterministic Dataset, never the Ledger, so its output is
   stable without a running database. The wire shapes here are frozen;
   content changes require a contract-version bump and the matching
   TypeScript update in the same change."
  (:require [clojure.string :as str]
            [datomic-blockchain.data.dataset-loader :as dataset]
            [datomic-blockchain.traceability.journey :as journey]))

(def contract-version
  "Shared by both UI endpoints; bump both together when either changes."
  "2026-10-01")

(defn- batch-id->variant-key
  "Map a batch identifier to dataset variant key."
  [batch-id]
  (cond
    (str/starts-with? batch-id "UHT-CHOC") :chocolate
    (str/starts-with? batch-id "UHT-PLAIN") :plain
    (str/starts-with? batch-id "UHT-STRAW") :strawberry
    (or (str/starts-with? batch-id "MILK-THAI")
        (str/starts-with? batch-id "RAW-MILK")) :raw-milk
    :else nil))
(defn- trace-find-product
  "Find a product in the deterministic dataset by attribute value."
  [ds attr code]
  (first (filter #(= (get % attr) code)
                 (vals (:products ds)))))

(def ^:private ui-ontology-prefixes
  {:prov "http://www.w3.org/ns/prov#"
   :sba "https://example.org/sba/ontology/uht-traceability/"})

(def ^:private ui-ontology-classes
  [{:id "ProductBatch"
    :label "Product Batch"
    :type "entity"
    :prov-class "prov:Entity"
    :description "Traceable product batch such as a UHT milk batch."}
   {:id "Activity"
    :label "Activity"
    :type "activity"
    :prov-class "prov:Activity"
    :description "Supply-chain activity such as farming, processing, logistics, or retail display."}
   {:id "Agent"
    :label "Agent"
    :type "agent"
    :prov-class "prov:Agent"
    :description "Organization or actor responsible for a traceability activity."}
   {:id "Location"
    :label "Location"
    :type "location"
    :prov-class "prov:Location"
    :description "Facility, room, route endpoint, store, or shelf where an activity occurs."}
   {:id "Evidence"
    :label "Evidence"
    :type "evidence"
    :prov-class "prov:Entity"
    :description "Record, transaction, certificate, or observation supporting a traceability claim."}
   {:id "QRCode"
    :label "QR Code"
    :type "identifier"
    :prov-class "prov:Entity"
    :description "Public identifier used by consumers to request a product trace."}])

(def ^:private ui-ontology-properties
  [{:id "wasGeneratedBy"
    :label "prov:wasGeneratedBy"
    :source "ProductBatch"
    :target "Activity"}
   {:id "wasAssociatedWith"
    :label "prov:wasAssociatedWith"
    :source "Activity"
    :target "Agent"}
   {:id "occurredAt"
    :label "sba:occurredAt"
    :source "Activity"
    :target "Location"}
   {:id "supportedBy"
    :label "sba:supportedBy"
    :source "ProductBatch"
    :target "Evidence"}
   {:id "identifiedBy"
    :label "sba:identifiedBy"
    :source "ProductBatch"
    :target "QRCode"}])

(defn- keyword-name
  [x]
  (cond
    (keyword? x) (name x)
    (string? x) x
    (nil? x) nil
    :else (str x)))

(defn- ui-node
  [id label type ontology-class attributes]
  {:id id
   :label label
   :type type
   :ontology-class ontology-class
   :attributes attributes})

(defn- ui-edge
  [source target relation]
  {:id (str source "->" target ":" relation)
   :source source
   :target target
   :label relation
   :type (last (str/split relation #":"))
   :prov-relation relation
   :attributes {}})

(defn- unique-by-id
  [items]
  (->> items
       (reduce (fn [acc item]
                 (if (contains? (:seen acc) (:id item))
                   acc
                   (-> acc
                       (update :seen conj (:id item))
                       (update :items conj item))))
               {:seen #{} :items []})
       :items))

(defn- location-label
  [location]
  (cond
    (nil? location) "Unknown Location"
    (:facility location) (str (:facility location)
                              (when-let [room (:room location)]
                                (str " / " room)))
    (:store location) (str (:store location)
                           (when-let [shelf (:shelf location)]
                             (str " / " shelf)))
    (and (:start location) (:end location)) (str (:start location) " to " (:end location))
    :else (str location)))

(defn- trace-product
  [ds kind code]
  (case kind
    "qr" (trace-find-product ds :uht/qr-code code)
    "batch" (trace-find-product ds :traceability/batch code)
    nil))

(defn build-trace-view
  "Build a stable frontend traceability view model from a Deterministic
   Dataset (default: the canonical classpath dataset; tests inject a
   fixture dataset).

   This contract is intentionally read-only and UI-oriented. It normalizes graph
   keys and uses PROV-O direction for provenance edges."
  ([code kind] (build-trace-view (dataset/load-dataset) code kind))
  ([ds code kind]
   (let [trace-kind (or (some-> kind keyword-name str/lower-case) "qr")
         product (trace-product ds trace-kind code)]
      (when-not (#{"qr" "batch"} trace-kind)
        (throw (ex-info "Unsupported trace kind"
                        {:error :unsupported-trace-kind
                         :status 400
                         :message "Trace kind must be qr or batch"
                         :kind trace-kind})))
      (when-not product
        (throw (ex-info "Trace product not found"
                        {:error :not-found
                         :status 404
                         :message (str "Trace product not found: " code)})))
      (let [batch-id (:traceability/batch product)
            product-label (or (:traceability/product product)
                              (:rdfs/label product)
                              batch-id)
            variant-key (batch-id->variant-key batch-id)
            journey (when variant-key
                      (journey/stages-from-dataset ds variant-key))]
        (when-not journey
          (throw (ex-info "Journey not found for product"
                          {:error :journey-not-found
                           :status 404
                           :message (str "Journey not found for product: " batch-id)})))
        (let [product-node (ui-node batch-id
                                    product-label
                                    "entity"
                                    "prov:Entity"
                                    {:batch-id batch-id
                                     :product-name product-label
                                     :variant-name (:uht/variant-name product)
                                     :flavor (:uht/flavor product)
                                     :qr-code (:uht/qr-code product)})
              qr-node (ui-node (:uht/qr-code product)
                               (:uht/qr-code product)
                               "identifier"
                               "sba:QRCode"
                               {:qr-code (:uht/qr-code product)})
              stage-nodes (mapv (fn [stage]
                                  (let [stage-id (keyword-name (:stage stage))
                                        activity (:activity stage)]
                                    (ui-node stage-id
                                             (:rdfs/label activity)
                                             "activity"
                                             "prov:Activity"
                                             {:stage stage-id
                                              :activity (:rdfs/label activity)
                                              :location (get-in activity [:uht/location])
                                              :timestamp (get-in activity [:prov/startedAtTime])})))
                                journey)
              location-nodes (mapv (fn [stage]
                                     (let [stage-id (keyword-name (:stage stage))
                                           location (get-in stage [:activity :uht/location])
                                           id (str stage-id "-location")]
                                       (ui-node id
                                                (location-label location)
                                                "location"
                                                "prov:Location"
                                                {:stage stage-id
                                                 :location location})))
                                   journey)
              stage-edges (mapv (fn [stage]
                                  (let [stage-id (keyword-name (:stage stage))]
                                    (ui-edge batch-id stage-id "prov:wasGeneratedBy")))
                                journey)
              location-edges (mapv (fn [stage]
                                     (let [stage-id (keyword-name (:stage stage))]
                                       (ui-edge stage-id
                                                (str stage-id "-location")
                                                "sba:occurredAt")))
                                   journey)
              qr-edge (ui-edge batch-id (:uht/qr-code product) "sba:identifiedBy")]
          {:contract-version contract-version
           :source "deterministic-uht-dataset"
           :query {:kind trace-kind
                   :code code}
           :product {:batch batch-id
                     :product product-label
                     :variant-name (:uht/variant-name product)
                     :flavor (:uht/flavor product)
                     :qr-code (:uht/qr-code product)}
           :stages (mapv (fn [stage]
                           (let [activity (:activity stage)]
                             {:stage (keyword-name (:stage stage))
                              :activity (:rdfs/label activity)
                              :location (get-in activity [:uht/location])
                              :time (get-in activity [:prov/startedAtTime])}))
                         journey)
           :graph {:nodes (unique-by-id (concat [product-node qr-node]
                                                stage-nodes
                                                location-nodes))
                   :edges (vec (concat stage-edges
                                       location-edges
                                       [qr-edge]))}})))))

(defn ontology-vocabulary
  "The prefix/class/property vocabulary the Trace View renders."
  []
  {:contract-version contract-version
   :prefixes ui-ontology-prefixes
   :classes ui-ontology-classes
   :properties ui-ontology-properties})
