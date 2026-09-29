(ns datomic-blockchain.traceability.completeness
  "Completeness assessment of a provenance path.

  THE definition of a provenance path in this companion: the derivation
  chain (:prov/wasDerivedFrom) enumerated from an entity, each hop's
  evidence read from the hop entity's :prov/wasGeneratedBy activity,
  its :prov/wasAssociatedWith agents, and the activity's time bounds.
  The recall rubric stamps this shape; history/graph views are adapters
  over the same relations.

  assess-completeness is the public interface — the score and named
  missing evidence in the shape handlers and the evaluation harness
  consume. score-provenance-paths is the path-level seam underneath it."
  (:require [taoensso.timbre :as log]
            [clojure.string :as str]
            [datomic.api :as d]
            [datomic-blockchain.query.graph :as graph]
            [datomic-blockchain.query.provenance :as prov-query])
  (:import [java.util Date]))

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
    (prov-query/entity-eid-by-uuid db entity-id)

    (string? entity-id)
    (when-let [uuid (prov-query/parse-uuid-safe entity-id)]
      (resolve-entity-id db uuid))

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
  [context weights]
  (let [{:keys [entity activity agents]} context
        started (:prov/startedAtTime activity)
        ended (:prov/endedAtTime activity)
        signals {:entity-present (boolean entity)
                 :entity-type (when entity (some? (:prov/entity-type entity)))
                 :activity-present (boolean activity)
                 :agent-present (when activity (boolean (seq agents)))
                 :time-start-present (when activity (some? started))
                 :time-order-valid (when activity
                                     (and started ended
                                          (not (.after ^Date started ^Date ended))))}
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
                                           weights)
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

(defn- assess-paths
  "Path-level detail under assess-completeness: ranked paths with per-hop
   evidence, starting at entity id."
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

(defn assess-completeness
  "Completeness score and named missing evidence for a provenance path —
   the interface the QR interface, the authenticated trace, and the
   labeled-path evaluation harness all call.

   Returns:
     {:status :assessed | :entity-absent
      :completeness-score double in [0 1]      ; 0.0 when :entity-absent
      :missing-evidence vector of signal-name strings from the best path
      :path-count how many provenance paths were found}

   Contract the recall rubric must encode:
   - Weights default to default-weights (the published six-signal
     calibration); pass {:weights {...}} to override or zero a signal
     for ablation.
   - A signal that cannot be evaluated because its prerequisite evidence
     is absent (agent, start time, time-order when there is no generating
     activity) is unknown, not missing: it is excluded from the
     denominator. A hop with no generating activity therefore scores
     0.5, not 0."
  ([db entity-id]
   (assess-completeness db entity-id {}))
  ([db entity-id opts]
   (let [result (assess-paths db entity-id opts)]
     (if (:error result)
       {:status :entity-absent
        :completeness-score 0.0
        :missing-evidence []
        :path-count 0}
       (let [best (get-in result [:confidence :best-path])
             score (double (or (:confidence best) 0.0))
             missing (->> (or (:hops best) [])
                          (mapcat :missing)
                          (map name)
                          distinct
                          vec)]
         {:status :assessed
          :completeness-score score
          :missing-evidence missing
          :path-count (get-in result [:confidence :path-count] 0)})))))
