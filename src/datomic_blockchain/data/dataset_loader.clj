(ns datomic-blockchain.data.dataset-loader
  "EDN Dataset Loader for the UHT milk supply chain dataset.

   Loads the canonical dataset from the classpath (resources/ is on the
   classpath), so loading does not depend on the working directory.
   Results are cached in-process per resource path."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [taoensso.timbre :as log]))

;; ============================================================================
;; Dataset Loading
;; =============================================================================

(def default-dataset-resource
  "Classpath resource for the canonical UHT dataset."
  "datasets/uht-supply-chain/data.edn")

(defonce ^:private dataset-cache (atom {}))

(defn clear-dataset-cache!
  "Clear the in-process EDN dataset cache.

   Useful for tests or long-running development sessions after editing the
   canonical dataset file."
  []
  (reset! dataset-cache {}))

(defn- resolve-source
  "Resolve a classpath resource first, then a filesystem path."
  [resource-or-path]
  (or (io/resource resource-or-path)
      (let [f (io/file resource-or-path)]
        (when (.exists f) f))))

(defn load-dataset
  "Load the UHT milk supply chain dataset from an EDN resource.

   Takes a classpath resource (default the canonical dataset) or a
   filesystem path; results are cached per resource path. Returns:
   {:agents {key => agent-data}
    :products {key => product-data}
    :activities {key => activity-data}}"
  ([] (load-dataset default-dataset-resource))
  ([resource-or-path]
   (if-let [cached (get @dataset-cache resource-or-path)]
     cached
     (let [source (resolve-source resource-or-path)]
       (when-not source
         (throw (ex-info "Dataset not found on classpath or filesystem"
                         {:resource resource-or-path})))
       (let [data (edn/read-string (slurp source))]
         (log/info "Dataset loaded:"
                   (count (:agents data)) "agents,"
                   (count (:products data)) "products,"
                   (count (:activities data)) "activities")
         (get (swap! dataset-cache assoc resource-or-path data)
              resource-or-path))))))
