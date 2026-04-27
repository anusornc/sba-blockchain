(ns datomic-blockchain.traceability.confidence
  "Provenance confidence scoring with explainable evidence.

  Computes confidence for provenance paths based on completeness and
  temporal consistency of PROV-O evidence."
  (:require [taoensso.timbre :as log]
            [clojure.string :as str]
            [datomic.api :as d]
            [datomic-blockchain.query.graph :as graph])
  (:import [java.util UUID Date]))

;; ============================================================================
;; Configuration
;; ============================================================================

(def default-weights
  "Default weights for evidence signals. Weights are normalized over
  available (non-nil) signals."
  {:entity-present 0.15
   :entity-type 0.10
   :activity-present 0.25
   :agent-present 0.20
   :time-start-present 0.20
   :time-order-valid 0.10})

;; ============================================================================
;; Utility
;; ============================================================================

(defn- resolve-entity-id
  "Resolve a PROV entity UUID or string to a Datomic entity id."
  [db entity-id]
  (cond
    (uuid? entity-id)
    (d/q '[:find ?e .
           :in $ ?id
           :where [?e :prov/entity ?id]]
         db entity-id)

    (string? entity-id)
    (let [uuid (try
                 (UUID/fromString entity-id)
                 (catch Exception _e nil))]
      (when uuid
        (resolve-entity-id db uuid)))

    (number? entity-id) entity-id

    :else entity-id))

(defn- normalize-weights
  [weights]
  (merge default-weights weights))

(defn- score-signals
  [signals weights]
  (let [available (filter (fn [[_ v]] (some? v)) signals)
        total-weight (reduce + 0.0 (map (fn [[k _]]
                                          (double (get weights k 0.0)))
                                        available))
        score (if (pos? total-weight)
                (/ (reduce + 0.0
                           (map (fn [[k v]]
                                  (if v
                                    (double (get weights k 0.0))
                                    0.0))
                                available))
                   total-weight)
                0.0)
        missing (->> available
                     (filter (fn [[_ v]] (false? v)))
                     (map first)
                     vec)
        unknown (->> signals
                     (filter (fn [[_ v]] (nil? v)))
                     (map first)
                     vec)]
    {:score score
     :missing missing
     :unknown unknown}))

;; ============================================================================
;; Evidence Collection
;; ============================================================================

(defn- activity-by-id
  [db activity-id]
  (when activity-id
    (when-let [activity-eid (d/q '[:find ?a .
                                   :in $ ?id
                                   :where [?a :prov/activity ?id]]
                                 db activity-id)]
      (d/entity db activity-eid))))

(defn- agent-by-id
  [db agent-id]
  (when agent-id
    (when-let [agent-eid (d/q '[:find ?a .
                                :in $ ?id
                                :where [?a :prov/agent ?id]]
                              db agent-id)]
      (d/entity db agent-eid))))

(defn- resolve-provenance-context
  [db entity-eid]
  (when-let [entity (d/entity db entity-eid)]
    (.touch entity)
    (let [activity-id (:prov/wasGeneratedBy entity)
          activity (activity-by-id db activity-id)
          agent-ids (when activity (:prov/wasAssociatedWith activity))
          agent-ids (cond
                      (nil? agent-ids) []
                      (coll? agent-ids) agent-ids
                      :else [agent-ids])
          agents (->> agent-ids
                      (map #(agent-by-id db %))
                      (remove nil?)
                      vec)]
      {:entity entity
       :activity activity
       :agents agents})))

(defn- evidence-for-context
  [context weights opts]
  (let [{:keys [entity activity agents]} context
        started (:prov/startedAtTime activity)
        ended (:prov/endedAtTime activity)
        policy-fn (:policy-fn opts)
        policy-present (when policy-fn
                         (boolean (policy-fn entity)))
        signals {:entity-present (boolean entity)
                 :entity-type (when entity (some? (:prov/entity-type entity)))
                 :activity-present (boolean activity)
                 :agent-present (when activity (seq agents))
                 :time-start-present (when activity (some? started))
                 :time-order-valid (when activity
                                     (and started ended
                                          (not (.after ^Date started ^Date ended))))
                 :policy-present policy-present}
        weights (normalize-weights weights)
        scoring (score-signals signals weights)]
    (merge scoring
           {:signals signals
            :entity-id (when entity (:db/id entity))
            :prov-entity-id (when entity (:prov/entity entity))
            :activity-id (when activity (:prov/activity activity))
            :agent-ids (when (seq agents)
                         (mapv :prov/agent agents))})))

;; ============================================================================
;; Path Enumeration
;; ============================================================================

(defn- derive-paths
  [start-id parent-fn {:keys [max-depth max-paths]}]
  (let [max-depth (or max-depth 6)
        max-paths (or max-paths 20)]
    (letfn [(expand [path visited depth]
              (if (>= depth max-depth)
                [path]
                (let [parents (remove visited (parent-fn (last path)))]
                  (if (seq parents)
                    (mapcat (fn [parent]
                              (expand (conj path parent)
                                      (conj visited parent)
                                      (inc depth)))
                            parents)
                    [path]))))]
      (take max-paths (expand [start-id] #{start-id} 0)))))

;; ============================================================================
;; Public API
;; ============================================================================

(defn score-provenance-paths
  "Score provenance paths using provided resolver and parent functions.

  Options:
  - :resolver (fn [entity-id] => {:entity :activity :agents})
  - :parent-fn (fn [entity-id] => [parent-ids])
  - :weights overrides for default weights
  - :max-depth, :max-paths"
  [start-id {:keys [resolver parent-fn weights] :as opts}]
  (let [resolver (or resolver (fn [_] nil))
        parent-fn (or parent-fn (fn [_] []))
        paths (derive-paths start-id parent-fn opts)
        scored (mapv (fn [path]
                       (let [hops (mapv #(evidence-for-context
                                           (resolver %)
                                           weights
                                           opts)
                                        path)
                             avg-score (if (seq hops)
                                         (/ (reduce + (map :score hops))
                                            (count hops))
                                         0.0)]
                         {:path path
                          :confidence avg-score
                          :hops hops}))
                     paths)
        ranked (vec (sort-by (comp - :confidence) scored))]
    {:paths ranked
     :best-path (first ranked)
     :path-count (count ranked)}))

(defn provenance-confidence
  "Compute confidence scores for provenance paths starting at entity id."
  [db entity-id opts]
  (let [entity-eid (resolve-entity-id db entity-id)]
    (if (nil? entity-eid)
      {:error :entity-not-found
       :message "Entity not found"}
      (let [resolver (fn [eid] (resolve-provenance-context db eid))
            parent-fn (fn [eid] (graph/get-parents db eid))
            result (score-provenance-paths entity-eid
                                           (assoc opts
                                                  :resolver resolver
                                                  :parent-fn parent-fn))
            entity (d/entity db entity-eid)]
        {:entity-id entity-eid
         :prov-entity-id (:prov/entity entity)
         :confidence result}))))
