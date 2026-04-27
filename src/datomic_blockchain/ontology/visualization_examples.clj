(ns datomic-blockchain.ontology.visualization-examples
  "Examples and utilities for knowledge graph visualization
  Demonstrates how to use the KB graph building functions with various visualization libraries"
  (:require [taoensso.timbre :as log]
            [datomic-blockchain.ontology.kb :as kb]
            [datomic-blockchain.query.graph :as graph]))

;; ============================================================================
;; D3.js Visualization Helpers
;; ============================================================================

(defn d3-force-layout-data
  "Prepare data for D3.js force-directed graph
  Returns JSON-serializable map with nodes and links

  Example usage:
    (def kg (kb/build-kg [entity-id] 2))
    (d3-force-layout-data kg)
    ;; => {:nodes [{:id \"...\" :name \"...\" :group \"entity\"}]
    ;;     :links [{:source \"...\" :target \"...\" :value 1}]}
  "
  [kg]
  (kg/kg->d3 kg))

(defn d3-tree-layout-data
  "Prepare data for D3.js tree layout
  Returns hierarchical structure suitable for d3.tree()

  Example usage:
    (d3-tree-layout-data db root-entity-id)
  "
  [db root-id]
  (let [root (graph/get-entity db root-id)
        children-fn (fn [entity-id]
                       (mapv (fn [child-id]
                              (let [child (graph/get-entity db child-id)]
                                {:id (str child-id)
                                 :name (or (:prov/entity-type child)
                                            (:prov/activity-type child)
                                            (str child-id))
                                 :type (cond
                                           (:prov/entity child) :entity
                                           (:prov/activity child) :activity
                                           (:prov/agent child) :agent
                                           :else :unknown)
                                 :children (lazy-seq
                                            (map (fn [c]
                                                  (update-in c [:children]
                                                           #(when (seq %) %)))
                                                 (children-fn (:id c))))}))
                            (graph/get-children db entity-id)))]
    {:id (str root-id)
     :name (or (:prov/entity-type root)
               (:prov/activity-type root)
               (str root-id))
     :type (cond
            (:prov/entity root) :entity
            (:prov/activity root) :activity
            (:prov/agent root) :agent
            :else :unknown)
     :children (when-let [child-ids (seq (graph/get-children db root-id))]
                (mapv (fn [child-id]
                       (let [child (graph/get-entity db child-id)]
                         {:id (str child-id)
                          :name (or (:prov/entity-type child)
                                     (:prov/activity-type child)
                                     (str child-id))
                          :type (cond
                                 (:prov/entity child) :entity
                                 (:prov/activity child) :activity
                                 (:prov/agent child) :agent
                                 :else :unknown)
                          :children (children-fn child-id)}))
                     child-ids))}))

;; ============================================================================
;; Cytoscape.js Visualization Helpers
;; ============================================================================

(defn cytoscape-elements
  "Prepare data for Cytoscape.js network visualization
  Returns {:elements [{:data {:id ...}} ...]}

  Example usage:
    (def kg (kb/build-kg [entity-id] 2))
    (cytoscape-elements kg)
    ;; => {:elements [{:data {:id \"...\" :label \"...\" :type \"entity\"}}
    ;;           {:data {:source \"...\" :target \"...\" :label \"wasDerivedFrom\"}}]}
  "
  [kg]
  (kb/kg->cytoscape kg))

(defn cytoscape-style-map
  "Generate Cytoscape.js style configuration
  Returns map with node and edge styling rules

  Example usage:
    (cytoscape-style-map)
    ;; => {:selector \"node[type='entity']\"
    ;;     :style {:background-color \"#4CAF50\" ...}}
  "
  []
  {:selector "node"
   :style {:label "data(label)"
            :width 60
            :height 60
            :text-valign "center"
            :text-halign "center"
            :font-size 10}}

   {:selector "node[type='entity']"
   :style {:background-color "#4CAF50"
            :shape "ellipse"}}

   {:selector "node[type='activity']"
   :style {:background-color "#2196F3"
            :shape "round-rectangle"}}

   {:selector "node[type='agent']"
   :style {:background-color "#FF9800"
            :shape "diamond"}}

   {:selector "edge"
   :style {:curve-style "bezier"
            :target-arrow-shape "triangle"
            :line-color "#999"
            :width 2}}

   {:selector "edge[relation='prov:wasDerivedFrom']"
   :style {:line-color "#666"
            :line-style "dashed"}}

   {:selector "edge[relation='prov:wasGeneratedBy']"
   :style {:line-color "#2196F3"
            :target-arrow-shape "vee"}})

;; ============================================================================
;; Graphviz DOT Export
;; ============================================================================

(defn export-dot-graph
  "Export knowledge graph to Graphviz DOT format
  Returns string that can be saved to .dot file and rendered

  Example usage:
    (def kg (kb/build-kg [entity-id] 2))
    (spit \"graph.dot\" (export-dot-graph kg))
    ;; Then run: dot -Tpng graph.dot -o graph.png
  "
  [kg]
  (kb/kg-export-graphviz kg))

(defn export-dot-hierarchical
  "Export as hierarchical graph (top-to-bottom)
  Better for showing supply chain flows"
  [kg]
  (let [node-strs (mapv (fn [node]
                           (str "  \"" (:id node) "\" "
                                "[label=\"" (:label node) "\" "
                                "shape=" (case (:type node)
                                           :entity "ellipse"
                                           :activity "box"
                                           :agent "diamond"
                                           "box")
                                " fillcolor=" (case (:type node)
                                                :entity "\"#C8E6C9\""
                                                :activity "\"#BBDEFB\""
                                                :agent "\"#FFE0B2\""
                                                "\"#EEEEEE\"")
                                " style=filled];"))
                         (:nodes kg))
        edge-strs (mapv (fn [edge]
                           (str "  \"" (:from edge) "\" -> \"" (:to edge) "\" "
                                "[label=\"" (:relation edge) "\"];"))
                         (:edges kg))]
    (str "digraph SupplyChain {\n"
         "  rankdir=TB;\n"
         "  node [style=filled, fontname=\"Arial\"];\n\n"
         (clojure.string/join "\n" node-strs)
         "\n\n"
         (clojure.string/join "\n" edge-strs)
         "\n}")))

(defn export-dot-radial
  "Export as radial graph (center outward)
  Good for highlighting a central entity and its connections"
  [kg center-id]
  (let [center-node (first (filter #(= (:id %) center-id) (:nodes kg)))
        node-strs (mapv (fn [node]
                           (str "  \"" (:id node) "\" "
                                "[label=\"" (:label node) "\" "
                                "shape=" (if (= (:id node) center-id)
                                           "doublecircle"
                                           "circle")
                                " fontsize=" (if (= (:id node) center-id)
                                               14
                                               10)
                                "];"))
                         (:nodes kg))
        edge-strs (mapv (fn [edge]
                           (str "  \"" (:from edge) "\" -> \"" (:to edge) "\" "
                                "[label=\"" (:relation edge) "\"];"))
                         (:edges kg))]
    (str "digraph RadialGraph {\n"
         "  rankdir=LR;\n"
         "  node [fontname=\"Arial\"];\n\n"
         (clojure.string/join "\n" node-strs)
         "\n\n"
         (clojure.string/join "\n" edge-strs)
         "\n}")))

;; ============================================================================
;; HTML/JavaScript Export for Web Visualization
;; ============================================================================

(defn export-html-d3
  "Generate complete HTML file with D3.js force-directed graph
  Returns HTML string that can be saved and opened in browser

  Example usage:
    (def kg (kb/build-kg [entity-id] 2))
    (spit \"viz.html\" (export-html-d3 kg))
  "
  [kg]
  (let [d3-data (pr-str (d3-force-layout-data kg))]
    (str "<!DOCTYPE html>\n"
         "<html>\n"
         "<head>\n"
         "  <meta charset=\"utf-8\">\n"
         "  <title>Knowledge Graph Visualization</title>\n"
         "  <script src=\"https://d3js.org/d3.v7.min.js\"></script>\n"
         "  <style>\n"
         "    body { margin: 0; font-family: Arial, sans-serif; }\n"
         "    svg { width: 100vw; height: 100vh; }\n"
         "    .node { stroke: #fff; stroke-width: 2px; }\n"
         "    .node.entity { fill: #4CAF50; }\n"
         "    .node.activity { fill: #2196F3; }\n"
         "    .node.agent { fill: #FF9800; }\n"
         "    .link { stroke: #999; stroke-opacity: 0.6; }\n"
         "    text { pointer-events: none; font-size: 12px; }\n"
         "  </style>\n"
         "</head>\n"
         "<body>\n"
         "  <script>\n"
         "    const data = " d3-data ";\n"
         "    const svg = d3.select('body').append('svg')\n"
         "      .attr('viewBox', [-400, -400, 800, 800]);\n"
         "    const simulation = d3.forceSimulation(data.nodes)\n"
         "      .force('link', d3.forceLink(data.links).id(d => d.id).distance(100))\n"
         "      .force('charge', d3.forceManyBody().strength(-300))\n"
         "      .force('center', d3.forceCenter(0, 0));\n"
         "    const link = svg.append('g')\n"
         "      .selectAll('line')\n"
         "      .data(data.links)\n"
         "      .join('line')\n"
         "      .attr('class', 'link');\n"
         "    const node = svg.append('g')\n"
         "      .selectAll('circle')\n"
         "      .data(data.nodes)\n"
         "      .join('circle')\n"
         "      .attr('class', d => 'node ' + d.group)\n"
         "      .attr('r', 20)\n"
         "      .call(d3.drag()\n"
         "        .on('start', dragstarted)\n"
         "        .on('drag', dragged)\n"
         "        .on('end', dragended));\n"
         "    node.append('text')\n"
         "      .text(d => d.name)\n"
         "      .attr('x', 0)\n"
         "      .attr('y', 5);\n"
         "    simulation.on('tick', () => {\n"
         "      link\n"
         "        .attr('x1', d => d.source.x)\n"
         "        .attr('y1', d => d.source.y)\n"
         "        .attr('x2', d => d.target.x)\n"
         "        .attr('y2', d => d.target.y);\n"
         "      node\n"
         "        .attr('cx', d => d.x)\n"
         "        .attr('cy', d => d.y);\n"
         "    });\n"
         "    function dragstarted(event, d) {\n"
         "      if (!event.active) simulation.alphaTarget(0.3).restart();\n"
         "      d.fx = d.x; d.fy = d.y;\n"
         "    }\n"
         "    function dragged(event, d) {\n"
         "      d.fx = event.x; d.fy = event.y;\n"
         "    }\n"
         "    function dragended(event, d) {\n"
         "      if (!event.active) simulation.alphaTarget(0);\n"
         "      d.fx = null; d.fy = null;\n"
         "    }\n"
         "  </script>\n"
         "</body>\n"
         "</html>")))

(defn export-html-cytoscape
  "Generate complete HTML file with Cytoscape.js network
  Returns HTML string with interactive network visualization

  Example usage:
    (def kg (kb/build-kg [entity-id] 2))
    (spit \"viz-cytoscape.html\" (export-html-cytoscape kg))
  "
  [kg]
  (let [cy-data (pr-str (cytoscape-elements kg))]
    (str "<!DOCTYPE html>\n"
         "<html>\n"
         "<head>\n"
         "  <meta charset=\"utf-8\">\n"
         "  <title>Knowledge Graph - Cytoscape</title>\n"
         "  <script src=\"https://cdnjs.cloudflare.com/ajax/libs/cytoscape/3.23.0/cytoscape.min.js\"></script>\n"
         "  <style>\n"
         "    #cy { width: 100vw; height: 100vh; position: absolute; left: 0; top: 0; }\n"
         "    .info { position: absolute; top: 10px; left: 10px; background: rgba(255,255,255,0.9); padding: 10px; border-radius: 5px; }\n"
         "  </style>\n"
         "</head>\n"
         "<body>\n"
         "  <div class=\"info\">Knowledge Graph Visualization</div>\n"
         "  <div id=\"cy\"></div>\n"
         "  <script>\n"
         "    const cy = cytoscape({\n"
         "      container: document.getElementById('cy'),\n"
         "      data: " cy-data ",\n"
         "      style: [\n"
         "        { selector: 'node', style: { 'label': 'data(label)', 'width': 60, 'height': 60 } },\n"
         "        { selector: \"node[type='entity']\", style: { 'background-color': '#4CAF50', 'shape': 'ellipse' } },\n"
         "        { selector: \"node[type='activity']\", style: { 'background-color': '#2196F3', 'shape': 'round-rectangle' } },\n"
         "        { selector: \"node[type='agent']\", style: { 'background-color': '#FF9800', 'shape': 'diamond' } },\n"
         "        { selector: 'edge', style: { 'curve-style': 'bezier', 'target-arrow-shape': 'triangle', 'width': 2 } }\n"
         "      ],\n"
         "      layout: { name: 'cose', animate: false },\n"
         "      zoom: 1,\n"
         "      pan: { x: 0, y: 0 }\n"
         "    });\n"
         "  </script>\n"
         "</body>\n"
         "</html>")))

;; ============================================================================
;; Analysis and Statistics Helpers
;; ============================================================================

(defn analyze-graph
  "Perform comprehensive graph analysis
  Returns statistics and insights about the knowledge graph

  Example usage:
    (analyze-graph kg)
    ;; => {:node-count 25
    ;;     :edge-count 42
    ;;     :type-distribution {:entity 15 :activity 8 :agent 2}
    ;;     :connected-components [...]
    ;;     :average-path-length 2.4}
  "
  [kg]
  (let [stats (kg/kg-stats kg)
        components (kg/kg-find-connected-components kg)
        component-count (count components)]
    (merge stats
           {:connected-component-count component-count
            :components-by-size (sort > (map count components))
            :is-connected (= component-count 1)})))

(defn find-critical-nodes
  "Find nodes with high connectivity (potential bottlenecks or hubs)
  Returns nodes sorted by degree

  Example usage:
    (find-critical-nodes kg)
    ;; => [{:id \"...\" :degree 15 :type :entity} ...]
  "
  [kg]
  (let [node-degrees (frequencies (concat (map :from (:edges kg))
                                          (map :to (:edges kg))))
        nodes-by-degree (sort-by :desc > (map (fn [[id degree]]
                                                  {:id id
                                                   :degree degree
                                                   :type (:type (first (filter #(= (:id %) id)
                                                                                         (:nodes kg))))})
                                                node-degrees))]
    {:highest-degree (first nodes-by-degree)
     :top-nodes (take 10 nodes-by-degree)
     :average-degree (/ (reduce + (map :degree nodes-by-degree)) 0.0
                        (max 1 (count nodes-by-degree)))}))

;; ============================================================================
;; Usage Examples
;; ============================================================================

(comment
  ;; Basic knowledge graph building
  (require '[datomic-blockchain.ontology.kb :as kb])
  (require '[datomic-blockchain.ontology.visualization-examples :as viz])

  ;; Initialize KB
  (kb/init-kb (dev/conn))

  ;; Build knowledge graph from entity
  (def entity-id some-uuid)
  (def kg (kb/build-kg [entity-id] 2))

  ;; Get graph statistics
  (viz/analyze-graph kg)

  ;; Export to various formats
  (spit "graph.dot" (viz/export-dot-graph kg))
  (spit "viz.html" (viz/export-html-d3 kg))
  (spit "viz-cytoscape.html" (viz/export-html-cytoscape kg))

  ;; Find critical nodes (hubs, bottlenecks)
  (viz/find-critical-nodes kg)

  ;; Find shortest path between two entities
  (viz/kg-find-shortest-path kg entity-a entity-b)

  ;; Filter graph by type
  (def entities-only (kg/kg-filter-by-type kg [:entity]))

  ;; Convert for specific visualization libraries
  (def d3-data (viz/d3-force-layout-data kg))
  (def cytoscape-data (viz/cytoscape-elements kg))

  ;; Cytoscape style configuration
  (def cy-style (viz/cytoscape-style-map))
  )
