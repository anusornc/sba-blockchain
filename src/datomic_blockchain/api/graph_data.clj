(ns datomic-blockchain.api.graph-data
  "Graph data extraction and formatting for visualization
  Converts Datomic query results to graph format for frontend libraries"
  (:require [taoensso.timbre :as log]
            [datomic.api :as d]
            [datomic-blockchain.query.graph :as graph]))

;; ============================================================================
;; Graph Data Structures
;; ============================================================================

(defn create-node
  "Create a graph node with standard format"
  ([id label type]
   (create-node id label type {}))
  ([id label type attributes]
   {:id id
    :label label
    :type type
    :attributes (merge {:id id
                       :label label
                       :type type}
                      attributes)}))

(defn create-edge
  "Create a graph edge with standard format"
  ([source target label]
   (create-edge source target label {}))
  ([source target label attributes]
   {:source source
    :target target
    :label label
    :attributes (merge {:source source
                       :target target
                       :label label}
                      attributes)
    :id (str source "-" target "-" label)}))

;; ============================================================================
;; Entity to Node Conversion
;; ============================================================================

(defn entity->node
  "Convert Datomic entity to graph node"
  [db entity-id]
  (let [entity (d/pull db '[*] entity-id)]
    (if entity
      (create-node
       (str entity-id)
       (or (:prov/type entity)
           (:entity/type entity)
           "Entity")
       (or (:prov/type entity)
           :entity)
       (select-keys entity [:prov/entity
                           :prov/activity
                           :prov/agent
                           :traceability/product
                           :traceability/batch]))
      (log/warn "Entity not found:" entity-id))))

(defn relationship->edge
  "Convert Datomic relationship to graph edge"
  [db source-id target-id relationship-type]
  (create-edge
   (str source-id)
   (str target-id)
   (name relationship-type)
   {:relationship-type relationship-type}))

;; ============================================================================
;; Graph Building
;; ============================================================================

(defn build-entity-graph
  "Build graph data from entity and its neighbors"
  [db entity-id depth]
  (log/info "Building graph for entity:" entity-id "depth:" depth)
  (let [neighbors-map (graph/get-node-neighbors db entity-id)
        ;; Flatten neighbors map into sequence of [id rel] pairs
        neighbors (for [[rel ids] neighbors-map
                        id ids]
                      {:id id
                       :relationship rel})
        nodes (atom #{})
        edges (atom [])]

    ;; Add root node
    (swap! nodes conj (entity->node db entity-id))

    ;; Process neighbors
    (doseq [neighbor-data neighbors]
      (let [neighbor-id (:id neighbor-data)
            relationship-type (:relationship neighbor-data)
            neighbor-node (entity->node db neighbor-id)]

        (when neighbor-node
          (swap! nodes conj neighbor-node)
          (swap! edges conj (relationship->edge
                             db entity-id neighbor-id relationship-type)))))

    {:nodes (vec @nodes)
     :edges (vec @edges)
     :metadata {:entity-id (str entity-id)
                :depth depth
                :node-count (count @nodes)
                :edge-count (count @edges)}}))

(defn build-subgraph
  "Build subgraph with multiple entities"
  [db entity-ids depth]
  (log/info "Building subgraph for" (count entity-ids) "entities")
  (let [all-nodes (atom #{})
        all-edges (atom [])]

    ;; Build graph for each entity
    (doseq [entity-id entity-ids]
      (let [graph-data (build-entity-graph db entity-id depth)]
        (swap! all-nodes concat (:nodes graph-data))
        (swap! all-edges concat (:edges graph-data))))

    ;; Remove duplicates
    {:nodes (vec (distinct @all-nodes))
     :edges (vec (distinct @all-edges))
     :metadata {:entity-ids (mapv str entity-ids)
                :depth depth
                :node-count (count (distinct @all-nodes))
                :edge-count (count (distinct @all-edges))}}))

;; ============================================================================
;; Path Visualization
;; ============================================================================

(defn path->graph
  "Convert path to graph format"
  [db path]
  (log/info "Converting path to graph:" (count path) "nodes")
  (let [nodes (mapv (fn [entity-id]
                      (entity->node db entity-id))
                    path)
        edges (mapv (fn [[source target]]
                      (create-edge (str source) (str target) "connected"))
                    (partition 2 1 path))]
    {:nodes nodes
     :edges edges
     :metadata {:path-length (count path)
                :type "path"}}))

(defn paths->graph
  "Convert multiple paths to graph format"
  [db paths]
  (log/info "Converting" (count paths) "paths to graph")
  (let [all-nodes (atom #{})
        all-edges (atom [])]

    (doseq [path paths]
      (let [path-graph (path->graph db path)]
        (swap! all-nodes concat (:nodes path-graph))
        (swap! all-edges concat (:edges path-graph))))

    {:nodes (vec (distinct @all-nodes))
     :edges (vec (distinct @all-edges))
     :metadata {:path-count (count paths)
                :type "multi-path"}}))

;; ============================================================================
;; Timeline Visualization
;; ============================================================================

(defn build-timeline
  "Build timeline visualization data"
  [db entity-id]
  (log/info "Building timeline for entity:" entity-id)
  (let [ancestors (graph/get-ancestors db entity-id)
        descendants (graph/get-descendants db entity-id)
        all-events (concat ancestors descendants)]

    {:events (sort-by :timestamp all-events)
     :entity-id (str entity-id)
     :event-count (count all-events)
     :metadata {:ancestor-count (count ancestors)
                :descendant-count (count descendants)}}))

(defn event->timeline-event
  "Convert entity event to timeline format"
  [event]
  {:id (str (:id event))
   :timestamp (:timestamp event)
   :title (or (:prov/type event)
              (:activity/type event)
              "Event")
   :description (or (:description event)
                    (str (:prov/type event)))
   :type (or (:prov/type event)
             :event)
   :data (select-keys event [:prov/entity
                             :prov/activity
                             :prov/agent])})

;; ============================================================================
;; Traceability Graph
;; ============================================================================

(defn build-traceability-graph
  "Build specialized graph for supply chain traceability"
  [db product-id]
  (log/info "Building traceability graph for product:" product-id)
  (let [history (d/q '[:find [?entity ?activity ?agent ?time ?location]
                      :in $ ?product-id
                      :where
                      [?entity :traceability/product ?product-id]
                      [?entity :prov/wasGeneratedBy ?activity]
                      [?activity :prov/startedAtTime ?time]
                      [?activity :prov/wasAssociatedWith ?agent]
                      [?activity :traceability/location ?location]]
                    db product-id)

        nodes (atom [])
        edges (atom [])]

    ;; Process history events
    (doseq [[entity-id activity-id agent-id time location] history]
      (let [entity-node (entity->node db entity-id)
            activity-node (entity->node db activity-id)
            agent-node (entity->node db agent-id)]

        (swap! nodes conj entity-node activity-node agent-node)
        (swap! edges conj (create-edge (str entity-id) (str activity-id) "generatedBy"))
        (swap! edges conj (create-edge (str activity-id) (str agent-id) "associatedWith"))))

    {:nodes (vec (distinct @nodes))
     :edges (vec (distinct @edges))
     :metadata {:product-id (str product-id)
                :event-count (count history)
                :type "traceability"}}))

;; ============================================================================
;; Force Layout Helpers
;; ============================================================================

(defn prepare-force-layout
  "Prepare graph data for force-directed layout

  Uses deterministic circular layout based on node ID hash for
  reproducible positioning. The graph structure (nodes, edges) comes
  from actual Datomic queries - only the visualization coordinates are
  computed."
  [graph-data]
  (let [node-count (count (:nodes graph-data))
        ;; Calculate positions using deterministic circular layout
        nodes-with-positions (mapv (fn [node index]
                                    (let [;; Use hash of node ID for deterministic position
                                          angle (* 2 Math/PI (/ index (max 1 node-count)))
                                          radius 400
                                          center-x 500
                                          center-y 500]
                                      (merge node
                                             {:x (+ center-x (* radius (Math/cos angle)))
                                              :y (+ center-y (* radius (Math/sin angle)))
                                              :vx 0
                                              :vy 0})))
                                  (:nodes graph-data)
                                  (range))]
    {:nodes nodes-with-positions
     :links (mapv (fn [edge]
                    {:source (:source edge)
                     :target (:target edge)
                     :value 1})
                  (:edges graph-data))}))

(defn calculate-node-degree
  "Calculate degree (number of connections) for each node"
  [graph-data]
  (let [edge-counts (frequencies (concat
                                   (map :source (:edges graph-data))
                                   (map :target (:edges graph-data))))]
    (mapv (fn [node]
            (assoc node :degree (get edge-counts (:id node) 0)))
          (:nodes graph-data))))

;; ============================================================================
;; Graph Statistics
;; ============================================================================

(defn graph-statistics
  "Calculate graph statistics"
  [graph-data]
  (let [nodes (:nodes graph-data)
        edges (:edges graph-data)
        node-types (frequencies (map :type nodes))
        edge-types (frequencies (map :label edges))
        degrees (map :degree (calculate-node-degree graph-data))]
    {:node-count (count nodes)
     :edge-count (count edges)
     :node-types node-types
     :edge-types edge-types
     :avg-degree (/ (reduce + degrees) (max 1 (count degrees)))
     :max-degree (apply max 0 degrees)
     :density (/ (* 2 (count edges))
                 (* (count nodes) (dec (count nodes))))}))

;; ============================================================================
;; Export Formats
;; ============================================================================

(defn export-cytoscape
  "Export graph in Cytoscape.js format"
  [graph-data]
  {:elements (concat
              (mapv (fn [node]
                      {:data (select-keys node [:id :label :type])})
                    (:nodes graph-data))
              (mapv (fn [edge]
                      {:data (select-keys edge [:id :source :target :label])})
                    (:edges graph-data)))})

(defn export-d3
  "Export graph in D3.js format"
  [graph-data]
  {:nodes (mapv (fn [node]
                  {:id (:id node)
                   :label (:label node)
                   :group (:type node)})
                (:nodes graph-data))
   :links (mapv (fn [edge]
                  {:source (:source edge)
                   :target (:target edge)
                   :value 1})
                (:edges graph-data))})

;; ============================================================================
;; Development Helpers
;; ============================================================================

(comment
  ;; Build graph for entity
  ;; (def db (d/db conn))
  ;; (def entity-id #uuid "550e8400-e29b-41d4-a716-446655440000")

  ;; (def graph-data (build-entity-graph db entity-id 2))

  ;; Get statistics
  ;; (graph-statistics graph-data)

  ;; Prepare for force layout
  ;; (def force-data (prepare-force-layout graph-data))

  ;; Export formats
  ;; (def cytoscape-data (export-cytoscape graph-data))
  ;; (def d3-data (export-d3 graph-data))

  )
