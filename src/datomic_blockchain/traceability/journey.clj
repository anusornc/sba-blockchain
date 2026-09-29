(ns datomic-blockchain.traceability.journey
  "One read model for Journeys (ADR-0003 excludes benchmark paths).

   Two named adapters over one canonical Stage — a superset where the
   acting agent is optional — and no protocols: each source has exactly
   one adapter. Trace views project the canonical Stage to their own
   wire shapes."
  (:require [datomic-blockchain.query.provenance :as prov-query]))

(defn- variant->manufacturing-activity
  [variant-key]
  (case variant-key
    :chocolate :uht-chocolate-processing
    :plain :uht-plain-processing
    :strawberry :uht-strawberry-processing
    :uht-processing))

(defn stages-from-dataset
  "The four-stage Journey for a Variant from a Deterministic Dataset.
   Manufacturing names the Variant's own processing activity; the
   generic activity stands in when the dataset has none (raw-milk)."
  [ds variant-key]
  (let [product (get-in ds [:products variant-key])
        manufacturing (or (get-in ds [:activities
                                      (variant->manufacturing-activity variant-key)])
                          (get-in ds [:activities :uht-processing]))]
    (when product
      [{:stage :farm
        :activity (get-in ds [:activities :milking])
        :product (get-in ds [:products :raw-milk])}
       {:stage :manufacturing
        :activity manufacturing
        :product product}
       {:stage :logistics
        :activity (get-in ds [:activities :transport])
        :product product}
       {:stage :retail
        :activity (get-in ds [:activities :retail-sale])
        :product product}])))

(defn stages-from-ledger
  "The Journey stages for a product from the Ledger, in time order."
  [db product-id]
  (->> (or (prov-query/query-product-history db product-id) [])
       (sort-by (fn [[_ _e _a _ag time _loc]] (str time)))
       (mapv (fn [[_entity-eid _activity-eid activity-type agent-name time location]]
               {:stage activity-type
                :activity (str activity-type)
                :agent (str agent-name)
                :time (str time)
                :location location}))))
