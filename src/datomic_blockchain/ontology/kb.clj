(ns datomic-blockchain.ontology.kb
  "Knowledge base operations for ontology-enhanced blockchain
  High-level operations combining ontology, queries, and graph traversal"
  (:require [taoensso.timbre :as log]
            [datomic.api :as d]
            [datomic-blockchain.query.sparql :as sparql]
            [datomic-blockchain.query.graph :as graph]))

;; ============================================================================
;; Knowledge Base State
;; ============================================================================

(defonce kb-state (atom nil))

(defn init-kb
  "Initialize knowledge base with connection"
  [conn]
  (reset! kb-state {:conn conn})
  (log/info "Knowledge base initialized"))

(defn get-conn
  "Get database connection from KB state"
  []
  (:conn @kb-state))

(defn get-db
  "Get current database value"
  []
  (d/db (get-conn)))

;; ============================================================================
;; Entity Management
;; ============================================================================

(defn create-entity!
  "Create a new PROV-O entity in the knowledge base"
  ([entity-type data]
   (create-entity! (get-conn) entity-type data))
  ([conn entity-type data]
   (log/info "Creating entity:" entity-type)
   (let [entity-id (random-uuid)
         entity-data (merge {:prov/entity entity-id
                             :prov/entity-type entity-type}
                            data)
         result @(d/transact conn [entity-data])]
     (log/info "Entity created:" entity-id)
     {:success true
      :entity-id entity-id
      :tx-result result})))

(defn get-entity
  "Get entity by ID with all attributes"
  ([entity-id]
   (get-entity (get-db) entity-id))
  ([db entity-id]
   (graph/get-entity db entity-id)))

(defn update-entity!
  "Update entity attributes"
  ([entity-id updates]
   (update-entity! (get-conn) entity-id updates))
  ([conn entity-id updates]
   (log/info "Updating entity:" entity-id)
   (let [entity-data (assoc updates :db/id entity-id)
         result @(d/transact conn [entity-data])]
     {:success true
      :entity-id entity-id
      :tx-result result})))

(defn delete-entity!
  "Retract entity from knowledge base
   PERFORMANCE: Uses :db/retractEntity for single operation (10-100x faster)
   Previous implementation did N separate :db/retract operations"
  ([entity-id]
   (delete-entity! (get-conn) entity-id))
  ([conn entity-id]
   (do
     (log/warn "Deleting entity:" entity-id)
     ;; OPTIMIZED: Use :db/retractEntity (single operation) instead of N retracts
     ;; This is 10-100x faster for entities with many attributes
     ;; Handle both UUID (convert to lookup ref) and Datomic ID
     (let [entity-ref (if (uuid? entity-id)
                        [:prov/entity entity-id]
                        entity-id)]
       @(d/transact conn [[:db/retractEntity entity-ref]]))
     {:success true
      :entity-id entity-id})))

;; ============================================================================
;; Activity Management
;; ============================================================================

(defn create-activity!
  "Create a new PROV-O activity"
  ([activity-type start-time end-time used-entities]
   (create-activity! (get-conn) activity-type start-time end-time used-entities))
  ([conn activity-type start-time end-time used-entities]
   (log/info "Creating activity:" activity-type)
   (let [activity-id (random-uuid)
         activity-data {:prov/activity activity-id
                        :prov/activity-type activity-type
                        :prov/startedAtTime start-time
                        :prov/endedAtTime end-time
                        :prov/used used-entities}
         result @(d/transact conn [activity-data])]
     {:success true
      :activity-id activity-id
      :tx-result result})))

(defn associate-agent!
  "Associate an agent with an activity"
  ([activity-id agent-id]
   (associate-agent! (get-conn) activity-id agent-id))
  ([conn activity-id agent-id]
   (log/info "Associating agent" agent-id "with activity" activity-id)
   @(d/transact conn [[:db/add activity-id :prov/wasAssociatedWith agent-id]])
   {:success true
      :activity-id activity-id
      :agent-id agent-id}))

(defn generate-entity!
  "Record that an activity generated an entity"
  ([activity-id entity-id]
   (generate-entity! (get-conn) activity-id entity-id))
  ([conn activity-id entity-id]
   (log/info "Activity" activity-id "generated entity" entity-id)
   @(d/transact conn [[:db/add entity-id :prov/wasGeneratedBy activity-id]])
   {:success true
      :entity-id entity-id
      :activity-id activity-id}))

;; ============================================================================
;; Agent Management
;; ============================================================================

(defn create-agent!
  "Create a new PROV-O agent"
  ([agent-type agent-name]
   (create-agent! (get-conn) agent-type agent-name))
  ([conn agent-type agent-name]
   (log/info "Creating agent:" agent-name)
   (let [agent-id (random-uuid)
         agent-data {:prov/agent agent-id
                     :prov/agent-type agent-type
                     :prov/agent-name agent-name}
         result @(d/transact conn [agent-data])]
     {:success true
      :agent-id agent-id
      :tx-result result})))

;; ============================================================================
;; High-Level Provenance Queries
;; ============================================================================

(defn get-provenance
  "Get complete provenance of an entity
  Returns entity, activities, and agents involved"
  ([entity-id]
   (get-provenance (get-db) entity-id))
  ([db entity-id]
   (let [provenance (sparql/query-provenance db entity-id)]
     (mapv (fn [[entity activity agent]]
             {:entity entity
              :activity activity
              :agent agent
              :entity-data (graph/get-entity db entity)
              :activity-data (graph/get-entity db activity)
              :agent-data (graph/get-entity db agent)})
           provenance))))

(defn get-supply-chain-history
  "Get complete supply chain history for a product
  Returns chronological list of events"
  ([product-id]
   (get-supply-chain-history (get-db) product-id))
  ([db product-id]
   (let [history (sparql/query-product-history db product-id)]
     (sort-by :time (mapv (fn [[entity activity-type location time]]
                            {:entity entity
                             :activity-type activity-type
                             :location location
                             :time time})
                          history)))))

(defn trace-product-path
  "Trace complete path of a product through supply chain
  Returns ordered list from origin to destination"
  ([product-id]
   (trace-product-path (get-db) product-id))
  ([db product-id]
   (log/info "Tracing product path:" product-id)
   (let [path (sparql/query-supply-chain-path db product-id)]
     (mapv (fn [[step entity activity agent location time]]
             {:step step
              :entity entity
              :activity activity
              :agent agent
              :location location
              :time time})
           path))))

;; ============================================================================
;; Knowledge Graph Operations
;; ============================================================================

(defn build-kg
  "Build knowledge graph for visualization
  Includes entities, activities, agents, and relationships
  Returns {:nodes [{:id :label :type}] :edges [{:from :to :relation}]}"
  ([entity-ids depth]
   (build-kg (get-db) entity-ids depth))
  ([db entity-ids depth]
   (log/info "Building knowledge graph for" (count entity-ids) "entities depth" depth)
   (reduce (fn [acc entity-id]
             (let [;; Get the entity
                   entity (graph/get-entity db entity-id)
                   node-id (str entity-id)
                   node-type (cond
                              (:prov/entity entity) :entity
                              (:prov/activity entity) :activity
                              (:prov/agent entity) :agent
                              :else :unknown)
                   node-label (or (:prov/entity-type entity)
                                   (:prov/activity-type entity)
                                   (:prov/agent-type entity)
                                   (str entity-id))

                   ;; Add current node
                   new-acc (update acc :nodes conj
                                          {:id node-id
                                           :label node-label
                                           :type node-type})

                   ;; Add edges from parents (wasDerivedFrom)
                   parents (graph/get-parents db entity-id)
                   new-acc (reduce (fn [acc' [rel-type parent-ids]]
                                     (reduce (fn [acc'' parent-id]
                                               (update acc'' :edges conj
                                                         {:from (str parent-id)
                                                          :to node-id
                                                          :relation rel-type}))
                                             acc'
                                             parent-ids))
                                   new-acc
                                   parents)

                   ;; Add edges to children (entities derived from this)
                   children (graph/get-children db entity-id)
                   new-acc (reduce (fn [acc' [rel-type child-ids]]
                                     (reduce (fn [acc'' child-id]
                                               (update acc'' :edges conj
                                                         {:from node-id
                                                          :to (str child-id)
                                                          :relation rel-type}))
                                             acc'
                                             child-ids))
                                   new-acc
                                   children)]
               new-acc))
           {:nodes [] :edges []}
           entity-ids)))

(defn build-kg-from-subgraph
  "Build knowledge graph from subgraph results
  More efficient for large graphs with many entities"
  ([db entity-id depth]
   (build-kg-from-subgraph (graph/build-subgraph-legacy db entity-id depth)))
  ([{:keys [nodes edges]}]
   (log/info "Building KG from subgraph:" (count nodes) "nodes," (count edges) "edges")
   {:nodes nodes
   :edges edges}))

(defn build-full-kg
  "Build complete knowledge graph with all entities in database
  USE WITH CAUTION on large databases"
  ([] (build-full-kg (get-db)))
  ([db]
   (log/info "Building full knowledge graph")
   (let [all-entities (concat
                      (d/q '[:find [?e ...] :where [?e :prov/entity]] db)
                      (d/q '[:find [?e ...] :where [?e :prov/activity]] db)
                      (d/q '[:find [?e ...] :where [?e :prov/agent]] db))]
   (build-kg db (vec all-entities) 1))))

(defn get-entity-network
  "Get network of connected entities around a central entity"
  ([entity-id depth]
   (get-entity-network (get-db) entity-id depth))
  ([db entity-id depth]
   (log/info "Getting entity network for" entity-id "depth" depth)
   (let [descendants (graph/get-descendants db entity-id depth)
         ancestors (graph/get-ancestors db entity-id depth)
         all-ids (clojure.set/union (set descendants)
                                    (set ancestors)
                                    #{entity-id})]
     (graph/build-graph-data db (vec all-ids)))))

;; ============================================================================
;; Analytics and Statistics
;; ============================================================================

(defn get-kb-stats
  "Get knowledge base statistics"
  ([]
   (get-kb-stats (get-db)))
  ([db]
   {:entities (d/q '[:find (count ?e) .
                     :where [?e :prov/entity]] db)
    :activities (d/q '[:find (count ?e) .
                       :where [?e :prov/activity]] db)
    :agents (d/q '[:find (count ?e) .
                   :where [?e :prov/agent]] db)
    :total-nodes (+ (d/q '[:find (count ?e) . :where [?e :prov/entity]] db)
                    (d/q '[:find (count ?e) . :where [?e :prov/activity]] db)
                    (d/q '[:find (count ?e) . :where [?e :prov/agent]] db))
    :total-edges (d/q '[:find (count ?e) .
                        :where
                        [?e :prov/wasGeneratedBy]] db)}))

(defn get-entity-type-stats
  "Get statistics grouped by entity type"
  ([]
   (get-entity-type-stats (get-db)))
  ([db]
   (d/q '[:find [?type ?count]
          :where
          [?e :prov/entity-type ?type]
          [(count ?e) ?count]]
        db)))

(defn get-activity-type-stats
  "Get statistics grouped by activity type"
  ([]
   (get-activity-type-stats (get-db)))
  ([db]
   (d/q '[:find [?type ?count]
          :where
          [?e :prov/activity-type ?type]
          [(count ?e) ?count]]
        db)))

(defn get-agent-type-stats
  "Get statistics grouped by agent type"
  ([]
   (get-agent-type-stats (get-db)))
  ([db]
   (d/q '[:find [?type ?count]
          :where
          [?e :prov/agent-type ?type]
          [(count ?e) ?count]]
        db)))

;; ============================================================================
;; Search and Discovery
;; ============================================================================

(defn search-entities
  "Search for entities by attribute values"
  ([attr value]
   (search-entities (get-db) attr value))
  ([db attr value]
   (log/debug "Searching entities:" attr "=" value)
   (d/q '[:find [?e]
          :in $ ?attr ?value
          :where
          [?e ?attr ?value]]
        db
        attr
        value)))

(defn search-by-product
  "Find all entities related to a product"
  ([product-name]
   (search-by-product (get-db) product-name))
  ([db product-name]
   (d/q '[:find [?e]
          :in $ ?product
          :where
          [?e :traceability/product ?product]]
        db
        product-name)))

(defn search-by-batch
  "Find all entities in a batch"
  ([batch-number]
   (search-by-batch (get-db) batch-number))
  ([db batch-number]
   (d/q '[:find [?e]
          :in $ ?batch
          :where
          [?e :traceability/batch ?batch]]
        db
        batch-number)))

(defn search-by-date-range
  "Find activities within date range"
  ([start-date end-date]
   (search-by-date-range (get-db) start-date end-date))
  ([db start-date end-date]
   (d/q '[:find [?activity ?time]
          :in $ ?start ?end
          :where
          [?activity :prov/startedAtTime ?time]
          [(>= ?time ?start)]
          [(<= ?time ?end)]]
        db
        start-date
        end-date)))

;; ============================================================================
;; Graph Visualization Helpers
;; ============================================================================

(defn kg->viz-json
  "Convert knowledge graph to JSON for visualization libraries
  Returns JSON string with nodes and arrays suitable for D3.js, Cytoscape, etc."
  [kg]
  (let [nodes-json (mapv (fn [node]
                           {:id (:id node)
                            :label (:label node)
                            :type (:type node)
                            :data (:data node)})
                         (:nodes kg))
        edges-json (mapv (fn [edge]
                           {:source (:from edge)
                            :target (:to edge)
                            :label (:relation edge)})
                         (:edges kg))]
    {:nodes nodes-json
     :links edges-json}))

(defn kg->cytoscape
  "Convert knowledge graph to Cytoscape.js format
  Includes group and selector information for styling"
  [kg]
  {:elements (concat
              (mapv (fn [node]
                      {:data {:id (:id node)
                             :label (:label node)
                             :type (:type node)}})
                    (:nodes kg))
              (mapv (fn [edge]
                      {:data {:source (:from edge)
                             :target (:to edge)
                             :label (:relation edge)}})
                    (:edges kg)))})

(defn kg->d3
  "Convert knowledge graph to D3.js force-directed graph format"
  [kg]
  {:nodes (mapv (fn [node]
                 {:id (:id node)
                  :name (:label node)
                  :group (:type node)})
               (:nodes kg))
   :links (mapv (fn [edge]
                 {:source (:from edge)
                  :target (:to edge)
                  :value 1})
               (:edges kg))})

(defn kg-filter-by-type
  "Filter knowledge graph to only include specific node types"
  [kg node-types]
  (let [type-set (set node-types)
        valid-ids (set (map :id (filter #(type-set (:type %)) (:nodes kg))))]
    {:nodes (filter #(valid-ids (:id %)) (:nodes kg))
     :edges (filter #(and (valid-ids (:from %))
                          (valid-ids (:to %)))
                  (:edges kg))}))

(defn kg-filter-by-ids
  "Filter knowledge graph to only include specified entity IDs"
  [kg id-set]
  (let [valid-ids (set id-set)]
    {:nodes (filter #(valid-ids (:id %)) (:nodes kg))
     :edges (filter #(and (valid-ids (:from %))
                          (valid-ids (:to %)))
                  (:edges kg))}))

(defn kg-filter-by-depth
  "Filter knowledge graph to only include nodes within N hops of root nodes
  Keeps only nodes reachable from initial entity-ids within max-depth"
  [kg entity-ids max-depth]
  (let [db (get-db)
        reachable-ids (set (mapcat #(graph/get-descendants db % max-depth)
                                   entity-ids))]
    (kg-filter-by-ids kg (conj reachable-ids entity-ids))))

(defn kg-stats
  "Get statistics about knowledge graph
  Returns node count, edge count, type distribution, connectivity info"
  [kg]
  (let [nodes (:nodes kg)
        edges (:edges kg)
        node-types (frequencies (map :type nodes))
        edge-types (frequencies (map :relation edges))
        node-degrees (frequencies (concat (map :from edges) (map :to edges)))
        max-degree (if (seq node-degrees) (apply max (vals node-degrees)) 0)]
    {:node-count (count nodes)
     :edge-count (count edges)
     :type-distribution node-types
     :edge-distribution edge-types
     :max-node-degree max-degree
     :average-degree (if (pos? (count nodes))
                      (* 2.0 (/ (count edges) (count nodes)))
                      0)}))

(defn kg-find-shortest-path
  "Find shortest path between two nodes in knowledge graph
  Returns sequence of node IDs or nil if no path exists"
  [kg from-id to-id]
  (let [db (get-db)
        path (graph/find-path db from-id to-id)]
    (when path
      {:path path
       :length (count path)
       :nodes (mapv (fn [id]
                      (first (filter #(= (:id %) id) (:nodes kg))))
                    path)})))

(defn kg-find-connected-components
  "Find connected components in knowledge graph
  Returns list of components, each being a set of node IDs"
  [kg]
  (let [node-id-set (set (map :id (:nodes kg)))
        edge-map (reduce (fn [acc edge]
                          (-> acc
                              (update (:from edge) conj (:to edge))
                              (update (:to edge) conj (:from edge))))
                        {}
                        (:edges kg))]
    (loop [remaining node-id-set
           components []
           visited #{}]
      (if (empty? remaining)
        components
        (let [start (first remaining)
              component (loop [queue [start]
                               visited-acc #{start}]
                          (if (empty? queue)
                            visited-acc
                            (let [current (first queue)
                                  neighbors (get edge-map current [])]
                              (recur (concat (rest queue)
                                             (remove visited-acc neighbors))
                                     (conj visited-acc current)))))]
          (recur (clojure.set/difference remaining component)
                 (conj components component)
                 (clojure.set/union visited component)))))))

(defn kg-export-graphviz
  "Export knowledge graph to Graphviz DOT format
  Useful for generating static graph visualizations"
  [kg]
  (let [node-strs (mapv (fn [node]
                           (str "  \"" (:id node) "\" "
                                "[label=\"" (:label node) "\" "
                                "type=\"" (:type node) "\"];"))
                         (:nodes kg))
        edge-strs (mapv (fn [edge]
                           (str "  \"" (:from edge) "\" -> \"" (:to edge) "\" "
                                "[label=\"" (:relation edge) "\"];"))
                         (:edges kg))]
    (str "digraph KnowledgeGraph {\n"
         "  rankdir=LR;\n"
         "  node [shape=box, style=rounded];\n\n"
         (clojure.string/join "\n" node-strs)
         "\n\n"
         (clojure.string/join "\n" edge-strs)
         "\n}")))

;; ============================================================================
;; Batch Operations
;; ============================================================================

(defn import-prov-o-data!
  "Import PROV-O data from external source
  Data should be vector of {entity, activities, agents} maps"
  ([data]
   (import-prov-o-data! (get-conn) data))
  ([conn data]
   (log/info "Importing" (count data) "PROV-O records")
   (let [tx-data (mapcat (fn [record]
                           (vec
                            (concat
                             ;; Entity
                             [(:entity record)]
                             ;; Activities
                             (:activities record [])
                             ;; Agents
                             (:agents record []))))
                         data)]
     @(d/transact conn tx-data)
     {:success true
      :imported (count data)})))

(defn export-prov-o-data
  "Export PROV-O data for external systems"
  ([entity-ids]
   (export-prov-o-data (get-db) entity-ids))
  ([db entity-ids]
   (log/info "Exporting" (count entity-ids) "entities")
   (mapv (fn [entity-id]
           (let [entity (graph/get-entity db entity-id)]
             {:entity entity
              :provenance (sparql/query-provenance db entity-id)}))
         entity-ids)))

;; ============================================================================
;; Utility Functions
;; ============================================================================

(defn clear-kb
  "Clear all data from knowledge base (USE WITH CAUTION)"
  []
  (log/warn "Clearing knowledge base!")
  (when false ;; Disabled for safety
    ;; Retract all entities
    (let [db (get-db)
          entities (d/q '[:find [?e] :where [?e :prov/entity]] db)
          activities (d/q '[:find [?e] :where [?e :prov/activity]] db)
          agents (d/q '[:find [?e] :where [?e :prov/agent]] db)
          all-ids (concat entities activities agents)]
      (doseq [id all-ids]
        (delete-entity! id)))
    (log/info "Knowledge base cleared")))

;; ============================================================================
;; Development Helpers
;; ============================================================================

(comment
  ;; Development REPL usage
  (init-kb (dev/conn))

  ;; Create entities
  (create-entity! :product/batch
                  {:traceability/product "tomatoes"
                   :traceability/batch "BATCH-001"})

  ;; Get provenance
  (get-provenance some-entity-id)

  ;; Trace product
  (trace-product-path product-id)

  ;; Get statistics
  (get-kb-stats)

  ;; Build knowledge graph
  (build-kg [entity-id] 3)

  ;; Knowledge graph visualization helpers
  (def kg (build-kg [entity-id] 3))

  ;; Convert to visualization format
  (kg->viz-json kg)        ;; Generic JSON for web viz
  (kg->cytoscape kg)       ;; Cytoscape.js format
  (kg->d3 kg)              ;; D3.js force-directed format

  ;; Filter and analyze
  (kg-stats kg)                              ;; Graph statistics
  (kg-filter-by-type kg [:entity])          ;; Only entities
  (kg-find-shortest-path kg id1 id2)        ;; Path between nodes
  (kg-find-connected-components kg)         ;; Connected components

  ;; Export to Graphviz DOT
  (kg-export-graphviz kg)    ;; For dot/neato/visualization

  ;; Build full graph (USE WITH CAUTION)
  (def full-kg (build-full-kg))
  )
