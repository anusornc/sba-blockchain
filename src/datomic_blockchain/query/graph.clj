(ns datomic-blockchain.query.graph
  "Graph traversal APIs for knowledge graph navigation
  Provides algorithms for navigating PROV-O relationships"
  (:require [taoensso.timbre :as log]
            [clojure.set :as set]
            [clojure.string :as str]
            [datomic.api :as d])
  (:import [java.util UUID LinkedList]
           [java.util ArrayList]))

;; ============================================================================
;; Node and Edge Structures
;; ============================================================================

(defrecord GraphNode [id type data])
(defrecord GraphEdge [from to relation])

(defn graph-node
  "Create a graph node"
  [id type data]
  ->GraphNode {:id id
               :type type
               :data data})

(defn graph-edge
  "Create a graph edge"
  [from to relation]
  ->GraphEdge {:from from
               :to to
               :relation relation})

;; ============================================================================
;; Basic Graph Queries
;; ============================================================================

(defn get-entity
  "Get entity from database by ID - looks up by :prov/entity attribute first
   Handles both UUIDs (lookup by attribute) and Datomic numeric IDs (direct lookup)"
  [db entity-id]
  (cond
    ;; If it's a UUID, look up by :prov/entity attribute
    (uuid? entity-id)
    (when-let [entity (d/entity db [:prov/entity entity-id])]
      (.touch entity)
      entity)
    
    ;; If it's a number (Datomic ID), look up directly
    (number? entity-id)
    (when-let [entity (d/entity db entity-id)]
      (.touch entity)
      entity)
    
    ;; If it's a collection (list/set/vector of IDs), return nil - we can't resolve a collection
    (coll? entity-id)
    nil
    
    ;; Default case - try as lookup ref (but not for collections)
    :else
    (when-let [entity (d/entity db entity-id)]
      (.touch entity)
      entity)))

(defn get-entity-type
  "Get the type of an entity (Entity, Activity, or Agent)"
  [db entity-id]
  (if (get-entity db entity-id)
    :entity
    :unknown))

(defn get-node-neighbors
  "Get all neighboring nodes connected to this node
  Returns map of {relation-type [neighbor-ids]}"
  [db node-id]
  (let [entity (get-entity db node-id)]
    (when entity
      (reduce (fn [acc [k v]]
                ;; Include PROV-O relation keywords (namespace = "prov")
                ;; but exclude :prov/entity itself (the entity's UUID)
                (if (and (keyword? k)
                         (= (namespace k) "prov")
                         (not= k :prov/entity)
                         (or (uuid? v) (coll? v)))
                  (let [neighbors (if (coll? v) v [v])]
                    (assoc acc k neighbors))
                  acc))
              {}
              entity))))

;; ============================================================================
;; Ancestors and Descendants
;; ============================================================================

(defn get-parents
  "Get direct parents (entities this entity was derived from)"
  [db entity-id]
  (let [entity (get-entity db entity-id)]
    (if-let [derived-from (:prov/wasDerivedFrom entity)]
      (let [;; Handle both cardinality-one (single UUID) and cardinality-many (set of UUIDs)
            parent-ids (cond
                         (set? derived-from) derived-from
                         (coll? derived-from) (set derived-from)
                         :else #{derived-from})
            ;; Use :find ?e . to get scalar values, not vectors
            parents (keep #(d/q '[:find ?e . :in $ ?v :where [?e :prov/entity ?v]] db %) parent-ids)]
        (vec parents))
      [])))

(defn get-children
  "Get direct children (entities derived from this entity)"
  ([db entity-id]
   (get-children db entity-id nil))
  ([db entity-id opts]
   (let [entity (get-entity db entity-id)
         prov-entity (:prov/entity entity)]
     (when prov-entity
       ;; Workaround for Datomic query issue with :prov/wasDerivedFrom
       ;; Use manual entity scan as reliable fallback.
       ;; Note: [?e ...] returns all results, [?e] returns only first
       (let [all-entities (d/q '[:find [?e ...]
                                 :where [?e :prov/entity]]
                               db)]
         (vec (keep #(when-let [derived (:prov/wasDerivedFrom (d/entity db %))]
                        ;; Handle both cardinality-one (single value) and cardinality-many (set)
                        (let [derived-set (if (set? derived) derived #{derived})]
                          (when (contains? derived-set prov-entity)
                            %)))
                    all-entities)))))))

(defn get-ancestors
  "Get all ancestors using BFS
  Optionally limit depth"
  ([db entity-id]
   (get-ancestors db entity-id nil))
  ([db entity-id max-depth]
   (loop [current #{entity-id}
          ancestors #{}
          depth 0]
     (if (or (empty? current)
             (and max-depth (>= depth max-depth)))
       ancestors
       (let [parents (set (mapcat #(get-parents db %) current))]
         (recur parents
                (clojure.set/union ancestors parents)
                (inc depth)))))))

(defn get-descendants
  "Get all descendants using BFS
  Optionally limit depth"
  ([db entity-id]
   (get-descendants db entity-id nil))
  ([db entity-id max-depth]
   (loop [current #{entity-id}
          descendants #{}
          depth 0]
     (if (or (empty? current)
             (and max-depth (>= depth max-depth)))
       descendants
       (let [children (set (mapcat #(get-children db %) current))]
         (recur children
                (clojure.set/union descendants children)
                (inc depth)))))))

;; ============================================================================
;; Path Finding
;; ============================================================================

(defn find-path
  "Find path between two entities using BFS
  Returns list of entity IDs forming the path"
  [db start-id end-id]
  (log/debug "Finding path from" start-id "to" end-id)
  (loop [queue [[start-id]]
         visited #{start-id}]
    (if-let [path (first queue)]
      (let [current (last path)]
        (cond
          (= current end-id)
          (do
            (log/debug "Path found:" (count path) "nodes")
            path)

          :else
          (let [neighbors (set (get-children db current))
                unvisited (clojure.set/difference neighbors visited)]
            (recur (concat (rest queue)
                           (map #(conj path %) unvisited))
                   (clojure.set/union visited unvisited)))))
      (do
        (log/debug "No path found")
        nil))))

(defn find-all-paths
  "Find all paths between two entities using DFS
  Returns list of paths"
  ([db start-id end-id]
   (find-all-paths db start-id end-id #{}))
  ([db start-id end-id visited]
   (cond
     (= start-id end-id)
     [[end-id]]

     (contains? visited start-id)
     []

     :else
     (let [neighbors (get-children db start-id)]
       (mapcat (fn [neighbor]
                 (map (fn [path] (cons start-id path))
                      (find-all-paths db neighbor end-id
                                     (conj visited start-id))))
               neighbors)))))

(defn shortest-path
  "Find shortest path between two entities
  Uses BFS and returns first (shortest) path found"
  [db start-id end-id]
  (find-path db start-id end-id))

;; ============================================================================
;; PROV-O Specific Traversals
;; ============================================================================

(defn trace-provenance
  "Trace provenance of an entity: find what generated it
  Returns chain: entity -> activity -> agent"
  [db entity-id]
  (d/q '[:find [?entity ?activity ?agent]
         :in $ ?entity-id
         :where
         [?entity :prov/entity ?entity-id]
         [?entity :prov/wasGeneratedBy ?activity]
         [?activity :prov/wasAssociatedWith ?agent]]
       db
       entity-id))

(defn trace-derivations
  "Trace all derivations from an entity
  Returns entities that were derived from this one"
  [db entity-id]
  ;; Use :find [?derived ...] to get all results, not just one
  (d/q '[:find [?derived ...]
         :in $ ?entity-id
         :where
         [?derived :prov/wasDerivedFrom ?entity-id]]
       db
       entity-id))

(defn trace-activity-chain
  "Trace chain of activities that used/derived an entity"
  [db entity-id]
  ;; Returns vector of [activity time agent] tuples
  (vec (d/q '[:find ?activity ?time ?agent
              :in $ ?entity-id
              :where
              [?activity :prov/used ?entity-id]
              [?activity :prov/startedAtTime ?time]
              [?activity :prov/wasAssociatedWith ?agent]]
            db
            entity-id)))

(defn trace-supply-chain
  "Trace complete supply chain path for a product
  Returns ordered list from origin to destination"
  [db product-entity-id]
  (log/info "Tracing supply chain for product:" product-entity-id)
  (let [path (find-path db product-entity-id product-entity-id)]
    (when path
      (mapv (fn [entity-id]
              (let [entity (get-entity db entity-id)]
                {:id entity-id
                 :type (:prov/entity-type entity)
                 :data entity}))
            path))))

(defn trace-full-history
  "Get complete history of an entity through time
  Includes all activities and agents involved"
  [db entity-id]
  (log/info "Getting full history for entity:" entity-id)
  (let [history (d/q '[:find [?entity ?activity ?agent ?time ?op]
                       :in $ ?entity-id
                       :where
                       [?entity :prov/entity ?entity-id]
                       [?entity :prov/wasGeneratedBy ?activity]
                       [?activity :prov/wasAssociatedWith ?agent]
                       [?activity :prov/startedAtTime ?time]]
                     db
                     entity-id)]
    (mapv (fn [[entity activity agent time op]]
            {:entity entity
             :activity activity
             :agent agent
             :time time
             :operation op})
          history)))

;; ============================================================================
;; Graph Statistics
;; ============================================================================

(defn count-nodes
  "Count total nodes in graph"
  [db]
  (+ (or (d/q '[:find (count ?e) .
                :where [?e :prov/entity]] db) 0)
     (or (d/q '[:find (count ?e) .
                :where [?e :prov/activity]] db) 0)
     (or (d/q '[:find (count ?e) .
                :where [?e :prov/agent]] db) 0)))

(defn count-edges
  "Count total derivation edges in graph"
  [db]
  (d/q '[:find (count ?e) .
         :where [?e :prov/wasDerivedFrom _]]
       db))

(defn node-degree
  "Get degree (number of connections) of a node"
  [db entity-id]
  (let [neighbors (get-node-neighbors db entity-id)]
    (reduce (fn [acc [rel neighbors-list]]
              (+ acc (count neighbors-list)))
            0
            neighbors)))

(defn- bfs-component
  "Helper: BFS from start node to find its entire connected component"
  [db start]
  (loop [queue [start]
         visited #{start}]
    (if (empty? queue)
      visited
      (let [current (first queue)
            children (set (get-children db current))
            parents (set (get-parents db current))
            neighbors (clojure.set/union children parents)
            unvisited (clojure.set/difference neighbors visited)]
        (recur (concat (rest queue) (seq unvisited))
               (clojure.set/union visited unvisited))))))

(defn connected-components
  "Find connected components in graph
   Returns map with :components, :component-count, :sizes, :largest-component-size"
  [db]
  (log/info "Computing connected components")
  (let [all-entities (concat
                      (d/q '[:find [?e ...] :where [?e :prov/entity]] db)
                      (d/q '[:find [?e ...] :where [?e :prov/activity]] db)
                      (d/q '[:find [?e ...] :where [?e :prov/agent]] db))
        all-nodes (set all-entities)]
    (loop [remaining all-nodes
           found []]
      (if (empty? remaining)
        (do
          (log/info "Found" (count found) "connected components")
          {:components found
           :component-count (count found)
           :sizes (sort > (map count found))
           :largest-component-size (if (empty? found) 0 (apply max (map count found)))})
        (let [component (bfs-component db (first remaining))
              rest (clojure.set/difference remaining component)]
          (recur rest (conj found component)))))))

(defn average-path-length
  "Calculate average shortest path length in graph

   Computes average shortest path length between all pairs of
   reachable nodes. This is a measure of graph connectivity and
   efficiency (smaller values = more tightly connected).

   Returns map with:
     :average-path-length - mean shortest path length
     :diameter - longest shortest path in graph
     :sample-size - number of nodes sampled
     :reachable-pairs - number of reachable node pairs

   Note: For large graphs, this is expensive. Consider sampling."
  ([db]
   (average-path-length db nil))
  ([db opts]
   (let [{:keys [max-samples max-pairs-per-node]
          :or {max-samples 100 max-pairs-per-node 50}} opts
         all-entities (take max-samples (concat
                                        (d/q '[:find [?e ...] :where [?e :prov/entity]] db)
                                        (d/q '[:find [?e ...] :where [?e :prov/activity]] db)
                                        (d/q '[:find [?e ...] :where [?e :prov/agent]] db)))
         all-nodes (vec all-entities)
         node-count (count all-nodes)]
     (log/info "Computing average path length")
     (if (zero? node-count)
       {:average-path-length 0
        :diameter 0
        :sample-size 0
        :reachable-pairs 0}
       (let [path-results (persistent!
                            (reduce (fn [acc! start-node]
                                      (let [targets (take max-pairs-per-node
                                                         (remove #(= % start-node) all-nodes))
                                            paths (keep #(find-path db start-node %) targets)
                                            path-lengths (map count paths)]
                                        (reduce conj! acc! path-lengths)))
                                    (transient [])
                                    all-nodes))
              total-paths (count path-results)
              avg-length (if (zero? total-paths)
                           0
                           (/ (reduce + path-results) total-paths))
              diameter (if (empty? path-results)
                         0
                         (apply max path-results))]
         (log/info "Computed" total-paths "paths, avg length:" avg-length)
         {:average-path-length avg-length
          :diameter diameter
          :sample-size node-count
          :reachable-pairs total-paths})))))

;; ============================================================================
;; Graph Visualization Data
;; ============================================================================

(defn build-graph-data
  "Build graph data structure for visualization
  Returns {nodes [{:id :type :data}] edges [{:from :to :relation}]}"
  [db entity-ids]
  (let [nodes (mapv (fn [id]
                      (let [entity (get-entity db id)]
                        {:id id
                         :type (:prov/entity-type entity)
                         :data (select-keys entity [:prov/entity-type
                                                    :traceability/product
                                                    :traceability/batch])}))
                    entity-ids)

        edges (reduce (fn [acc id]
                        (let [neighbors (get-node-neighbors db id)]
                          (reduce (fn [acc2 [rel rel-neighbors]]
                                    (concat acc2
                                            (mapv (fn [neighbor]
                                                    {:from id
                                                     :to neighbor
                                                     :relation rel})
                                                  rel-neighbors)))
                                  acc
                                  neighbors)))
                      []
                      entity-ids)]

    {:nodes nodes
     :edges edges}))

(defn build-subgraph-legacy
  "Build subgraph starting from entity with specified depth (legacy version)
   This function builds a full subgraph using descendants and is kept for
   backward compatibility."
  [db start-entity-id max-depth]
  (log/info "Building subgraph from" start-entity-id "depth" max-depth)
  (let [entity-ids (get-descendants db start-entity-id max-depth)]
    (build-graph-data db (conj entity-ids start-entity-id))))

(defn build-provenance-graph
  "Build provenance graph for an entity
  Shows all ancestors that contributed to this entity"
  [db entity-id max-depth]
  (log/info "Building provenance graph for" entity-id)
  (let [ancestor-ids (get-ancestors db entity-id max-depth)]
    (build-graph-data db (conj ancestor-ids entity-id))))

;; ============================================================================
;; Utility Functions
;; ============================================================================

(defn entity->string
  "Convert entity to readable string representation"
  [entity]
  (let [id (:db/id entity)
        entity-type (:prov/entity-type entity)
        product (:traceability/product entity)
        batch (:traceability/batch entity)]
    (str "#" id " " entity-type
         (when product (str " [" product "]"))
         (when batch (str " batch:" batch)))))

(defn path->string
  "Convert path to readable string"
  [db path]
  (str/join " -> "
            (mapv (fn [id]
                    (let [entity (get-entity db id)]
                      (if entity
                        (entity->string entity)
                        (str "#" id))))
                  path)))

;; ============================================================================
;; Graph Building for Visualization
;; ============================================================================

(defn build-subgraph
  "Build a subgraph around an entity for visualization
   Returns map with :nodes and :edges keys including parents and children"
  [db entity-id depth]
  (let [entity (get-entity db entity-id)
        root-eid (:db/id entity)
        max-depth (max 0 (or depth 1))]
    (if-not entity
      {:nodes [] :edges []}
      (letfn [(node-type [node]
                (cond
                  (:prov/entity node) :entity
                  (:prov/activity node) :activity
                  (:prov/agent node) :agent
                  :else :unknown))
              (node-label [node fallback]
                (or (:prov/entity-type node)
                    (:prov/activity-type node)
                    (:prov/agent-type node)
                    (:traceability/batch node)
                    (:prov/agent-name node)
                    (str fallback)))
              (node-id [node fallback]
                (str (or (:prov/entity node)
                         (:prov/activity node)
                         (:prov/agent node)
                         fallback)))
              (node-data [eid]
                (when-let [node (get-entity db eid)]
                  {:id (node-id node eid)
                   :label (node-label node eid)
                   :type (node-type node)}))]
        (let [entity-ids (-> #{root-eid}
                             (set/union (get-ancestors db root-eid max-depth))
                             (set/union (get-descendants db root-eid max-depth)))
              nodes-by-eid (into {}
                                 (keep (fn [eid]
                                         (when-let [node (node-data eid)]
                                           [eid node])))
                                 entity-ids)
              node-ids (into {} (map (fn [[eid node]] [eid (:id node)]) nodes-by-eid))
              edges (for [source-eid (keys nodes-by-eid)
                          child-eid (get-children db source-eid)
                          :when (contains? nodes-by-eid child-eid)]
                      {:from (get node-ids source-eid)
                       :to (get node-ids child-eid)
                       :relation :prov/wasDerivedFrom})]
          {:nodes (vec (vals nodes-by-eid))
           :edges (vec edges)})))))

;; ============================================================================
;; Development Helpers
;; ============================================================================

(comment
  ;; Development REPL usage
  (def db (dev/db))

  ;; Get neighbors
  (get-node-neighbors db some-entity-id)

  ;; Find path
  (find-path db entity-a entity-b)

  ;; Trace provenance
  (trace-provenance db product-id)

  ;; Build subgraph for visualization
  (build-subgraph-legacy db product-id 3)

  ;; Build provenance graph
  (build-provenance-graph db product-id 5))
