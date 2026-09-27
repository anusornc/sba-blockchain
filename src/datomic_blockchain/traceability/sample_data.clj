(ns datomic-blockchain.traceability.sample-data
  "Labeled-path seeding: the one module that turns the canonical UHT
   dataset into transactable PROV transactions.

   This is the seam issue 02's labeled paths plug into: the complete
   chocolate path (do-not-pull negative control) and its defect variants
   are data here, not forks of the topology. seed-uht-sample! is the
   adapter the dev handler and handler tests use; seed-labeled-path!
   seeds a variant with one defect injected at the transaction level.

   All ids are deterministic (UUID/nameUUIDFromBytes), so repeated seeds
   are idempotent per scope and variants coexist with the base sample in
   one database."
  (:require [clojure.instant :as instant]
            [clojure.string :as str]
            [datomic.api :as d]
            [datomic-blockchain.data.dataset-loader :as dataset]
            [taoensso.timbre :as log])
  (:import [java.util Date UUID]))

;; =============================================================================
;; Dataset source
;; =============================================================================

(def dataset-source
  "Classpath resource for the canonical UHT dataset (see dataset-loader)."
  dataset/default-dataset-resource)

;; =============================================================================
;; Defect vocabulary — the four first-class evidence defects
;; =============================================================================

(def ^:private defect->expected-missing
  "Each defect maps to the completeness signal the assessment interface
   must name in :missing-evidence when that defect is injected."
  {:missing-activity "activity-present"
   :missing-agent "agent-present"
   :missing-start-time "time-start-present"
   :invalid-time-order "time-order-valid"})

(defn labeled-path-defects
  "The defect types a labeled path can carry."
  []
  (vec (keys defect->expected-missing)))

(defn expected-missing-evidence
  "The signal name the completeness assessment must report for a defect."
  [defect]
  (get defect->expected-missing defect))

;; =============================================================================
;; Deterministic ids
;; =============================================================================

(defn- stable-uuid
  "Deterministic UUID for a seed id; scoped so variants coexist with the
   base sample in one database."
  ([id-str] (stable-uuid id-str nil))
  ([id-str scope]
   (UUID/nameUUIDFromBytes
    (.getBytes (str "uht-seed/" id-str (when scope (str "/" (name scope)))) "UTF-8"))))

(defn- ids-for
  "Deterministic PROV uuids for every agent/product/activity key in the
   dataset, optionally scoped to a defect variant."
  [ds scope]
  (let [mk (fn [prefix keys*]
             (into {} (map (fn [k] [k (stable-uuid (str prefix "/" (name k)) scope)]) keys*)))]
    {:agent-ids (mk "agent" (keys (:agents ds)))
     :product-ids (mk "entity" (keys (:products ds)))
     :activity-ids (mk "activity" (keys (:activities ds)))}))

;; =============================================================================
;; Dataset -> PROV translation
;; =============================================================================

(defn- as-date
  "Parse ISO-8601 string into java.util.Date."
  [v]
  (cond
    (instance? Date v) v
    (string? v) (instant/read-instant-date v)
    :else nil))

(defn- normalize-location
  "Convert dataset location map into a compact string for schema compatibility."
  [location]
  (cond
    (string? location) location
    (map? location)
    (let [address (:address location)
          district (:district location)
          province (:province location)]
      (or address
          (some->> [district province]
                   (remove nil?)
                   seq
                   (str/join ", "))
          "Unknown Location"))
    :else "Unknown Location"))

(defn- agent-type-for-key
  [k]
  (case k
    :farmer :organization/farmer
    :manufacturer :organization/manufacturer
    :logistics :organization/logistics
    :retailer :organization/retailer
    :consumer :organization/consumer
    :organization/other))

(defn- entity-type-for-key
  [k]
  (case k
    :raw-milk :product/raw-milk
    :chocolate :product/chocolate-uht
    :plain :product/plain-uht
    :strawberry :product/strawberry-uht
    :product/unknown))

(defn- activity-type-for-key
  [k]
  (case k
    :milking :activity/milking
    :uht-processing :activity/uht-processing
    :uht-chocolate-processing :activity/chocolate-processing
    :uht-plain-processing :activity/plain-processing
    :uht-strawberry-processing :activity/strawberry-processing
    :transport :activity/transport
    :retail-sale :activity/retail-sale
    :activity/unknown))

(defn- related-activity-for-variant
  [variant]
  (case variant
    :chocolate :uht-chocolate-processing
    :plain :uht-plain-processing
    :strawberry :uht-strawberry-processing
    nil))

(defn- variant-keys []
  [:chocolate :plain :strawberry])

;; =============================================================================
;; Sample transactions (pure data builders)
;; =============================================================================

(defn- agent-tx-for
  [ids ds]
  (mapv (fn [[k data]]
          (let [agent-id (get (:agent-ids ids) k)
                certs (or (:uht/certifications data) #{})
                cert-strings (mapv (fn [c] (if (keyword? c) (name c) (str c))) certs)]
            {:db/id (str "agent-" (name k))
             :prov/agent agent-id
             :prov/agent-name (or (:uht/agent-name data) "Unknown Agent")
             :prov/agent-type (or (:uht/agent-type data) (agent-type-for-key k))
             :traceability/location (normalize-location (:uht/location data))
             :traceability/certifications cert-strings}))
        (:agents ds)))

(defn- product-tx-for
  [ids ds scope]
  (let [suffix (when scope (str "-" (name scope)))]
    (mapv (fn [[k data]]
            (let [entity-id (get (:product-ids ids) k)
                  product-id (stable-uuid (str "product/" (name k)) scope)]
              (cond-> {:db/id (str "product-" (name k))
                       :prov/entity entity-id
                       :prov/entity-type (entity-type-for-key k)
                       :traceability/batch (str (or (:traceability/batch data) "UNKNOWN")
                                                (or suffix ""))
                       :traceability/product product-id
                       :traceability/product-name (or (:traceability/product data)
                                                     (:uht/variant-name data)
                                                     "Unknown Product")}
                (:uht/qr-code data)
                (assoc :traceability/qr-code (str (:uht/qr-code data) (or suffix ""))))))
          (:products ds))))

(defn- activity-tx-for
  [ids ds]
  (mapv (fn [[k data]]
          (let [activity-id (get (:activity-ids ids) k)
                started-at (or (as-date (:prov/startedAtTime data)) (Date.))
                ended-at (as-date (:prov/endedAtTime data))]
            (cond-> {:db/id (str "activity-" (name k))
                     :prov/activity activity-id
                     :prov/activity-type (activity-type-for-key k)
                     :prov/startedAtTime started-at}
              ended-at (assoc :prov/endedAtTime ended-at))))
        (:activities ds)))

(defn- relationship-tx-for
  "The hardcoded sample topology: milking -> raw milk -> variant
   processing -> variant product -> transport/retail, with the
   responsible agent per activity."
  [ids]
  (let [{:keys [agent-ids product-ids activity-ids]} ids
        raw-id (get product-ids :raw-milk)
        transport-id (get activity-ids :transport)
        retail-id (get activity-ids :retail-sale)
        common-links
        [[:db/add [:prov/activity (get activity-ids :milking)]
          :prov/wasAssociatedWith
          (get agent-ids :farmer)]
         [:db/add [:prov/entity raw-id]
          :prov/wasGeneratedBy
          (get activity-ids :milking)]]
        variant-links
        (mapcat
         (fn [variant]
           (let [product-id (get product-ids variant)
                 variant-activity-key (related-activity-for-variant variant)
                 variant-activity-id (get activity-ids variant-activity-key)]
             [[:db/add [:prov/activity variant-activity-id]
               :prov/used
               raw-id]
              [:db/add [:prov/activity variant-activity-id]
               :prov/wasAssociatedWith
               (get agent-ids :manufacturer)]
              [:db/add [:prov/entity product-id]
               :prov/wasGeneratedBy
               variant-activity-id]
              [:db/add [:prov/entity product-id]
               :prov/wasDerivedFrom
               raw-id]
              [:db/add [:prov/activity transport-id]
               :prov/used
               product-id]
              [:db/add [:prov/activity retail-id]
               :prov/used
               product-id]]))
         (variant-keys))
        logistics-links
        [[:db/add [:prov/activity transport-id]
          :prov/wasAssociatedWith
          (get agent-ids :logistics)]
         [:db/add [:prov/activity retail-id]
          :prov/wasAssociatedWith
          (get agent-ids :retailer)]]]
    (vec (concat common-links variant-links logistics-links))))

;; =============================================================================
;; Defect injection (pure transforms on the transaction groups)
;; =============================================================================

(defn- chocolate-hop-ids
  "The ids of the chocolate hop — the hop every defect is injected into."
  [ids]
  {:entity (get-in ids [:product-ids :chocolate])
   :activity (get-in ids [:activity-ids :uht-chocolate-processing])
   :agent (get-in ids [:agent-ids :manufacturer])})

(defn- inject-link-defect
  "Remove the relationship tuple(s) a defect removes."
  [defect rel-tx {:keys [entity activity agent]}]
  (case defect
    :missing-activity
    (vec (remove (fn [[_ _ attr value]]
                   (and (= attr :prov/wasGeneratedBy)
                        (= value activity)))
                 rel-tx))

    :missing-agent
    (vec (remove (fn [[_ entity-ref attr _]]
                   (and (= attr :prov/wasAssociatedWith)
                        (= entity-ref [:prov/activity activity])))
                 rel-tx))

    rel-tx))

(defn- inject-activity-defect
  "Rewrite the chocolate activity's transaction map for a defect."
  [defect activity-tx {:keys [activity]}]
  (if (contains? #{:missing-start-time :invalid-time-order} defect)
    (mapv (fn [tx]
            (if (= (:prov/activity tx) activity)
              (case defect
                :missing-start-time (dissoc tx :prov/startedAtTime)
                :invalid-time-order (assoc tx :prov/endedAtTime
                                           (Date. (- (.getTime ^Date (:prov/startedAtTime tx))
                                                     (* 3600 1000)))))
              tx))
          activity-tx)
    activity-tx))

(defn- apply-defect
  "Apply one defect to the transaction groups. Unknown defects pass the
   groups through unchanged."
  [defect groups ids]
  (if (get defect->expected-missing defect)
    (let [hop (chocolate-hop-ids ids)]
      (-> groups
          (update :relationship-tx #(inject-link-defect defect % hop))
          (update :activity-tx #(inject-activity-defect defect % hop))))
    groups))

;; =============================================================================
;; Transaction groups and anchors
;; =============================================================================

(defn- sample-tx-groups
  "Pure: dataset + optional defect (which also scopes the ids) ->
   transaction groups + anchors."
  ([ds] (sample-tx-groups ds nil))
  ([ds defect]
  (let [ids (ids-for ds defect)
        groups {:ids ids
                :agent-tx (agent-tx-for ids ds)
                :product-tx (product-tx-for ids ds defect)
                :activity-tx (activity-tx-for ids ds)
                :relationship-tx (relationship-tx-for ids)}
         groups (if defect (apply-defect defect groups ids) groups)
         suffix (when defect (str "-" (name defect)))
         chocolate-data (get-in ds [:products :chocolate])
         benchmark-batch (str (or (:traceability/batch chocolate-data)
                                  "UHT-CHOC-CM-2024-001")
                              (or suffix ""))
         benchmark-qr (str (or (:uht/qr-code chocolate-data)
                               "UHT-CHOC-2024-001-QR")
                           (or suffix ""))]
     (assoc groups
            :anchors {:qr-code benchmark-qr
                      :batch-id benchmark-batch
                      :entity-id (str (get-in ids [:product-ids :chocolate]))
                      :activity-id (str (get-in ids [:activity-ids :uht-chocolate-processing]))}))))

(defn- transact-groups!
  "Transact the four groups in dependency order."
  [conn {:keys [agent-tx product-tx activity-tx relationship-tx]}]
  @(d/transact conn agent-tx)
  @(d/transact conn product-tx)
  @(d/transact conn activity-tx)
  @(d/transact conn relationship-tx))

;; =============================================================================
;; Public interface
;; =============================================================================

(defn seed-uht-sample!
  "Seed the canonical UHT sample supply chain. Idempotent per database:
   deterministic ids upsert. Returns dataset meta, counts, benchmark
   anchors, and the product summary the dev handler echoes."
  [conn]
  (let [ds (dataset/load-dataset dataset-source)
        {:keys [agent-tx product-tx activity-tx relationship-tx anchors] :as groups}
        (sample-tx-groups ds)]
    (transact-groups! conn groups)
    (log/info "Loaded deterministic UHT sample data"
              {:agents (count agent-tx)
               :products (count product-tx)
               :activities (count activity-tx)
               :relationships (count relationship-tx)})
    (let [chocolate-data (get-in ds [:products :chocolate])]
      {:dataset-source (str "resources/" dataset-source)
       :dataset-meta (:meta ds)
       :counts {:agents (count agent-tx)
                :products (count product-tx)
                :activities (count activity-tx)
                :relationships (count relationship-tx)}
       :benchmark-anchors anchors
       :products [{:batch-id (:batch-id anchors)
                   :name (:traceability/product chocolate-data)
                   :stages ["Milking"
                            "Chocolate Processing"
                            "Cold Chain Transport"
                            "Retail Sale"]}]})))

(defn seed-labeled-path!
  "Seed the chocolate path with one defect injected at the transaction
   level. Variant ids are defect-scoped, so labeled paths coexist with
   the base sample (and each other) in one database.

   Returns the defect, the missing evidence the completeness assessment
   must name for it, and the anchors (qr/batch/entity/activity) of the
   labeled path."
  [conn defect]
  (let [ds (dataset/load-dataset dataset-source)
        {:keys [anchors] :as groups} (sample-tx-groups ds defect)]
    (transact-groups! conn groups)
    {:defect defect
     :expected-missing-evidence (expected-missing-evidence defect)
     :anchors anchors}))
