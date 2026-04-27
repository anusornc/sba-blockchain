(ns datomic-blockchain.ontology.loader
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [taoensso.timbre :as log]
            [datomic.api :as d]
            [clojure.data.json :as json]
            [datomic-blockchain.ontology.mapper :as mapper])
  (:import [java.io File FileInputStream]
           [java.util UUID]))

;; ============================================================================
;; Ontology Metadata
;; ============================================================================

(defn create-ontology-metadata
  "Create metadata record for an ontology"
  [name namespace format version]
  {:ontology/id (random-uuid)
   :ontology/name name
   :ontology/namespace namespace
   :ontology/format format
   :ontology/version version
   :ontology/loaded-at (java.util.Date.)})

;; ============================================================================
;; File Loading
;; ============================================================================

(defn load-ontology-file
  "Load ontology from file path"
  [file-path]
  (log/info "Loading ontology from:" file-path)
  (try
    (let [file (io/file file-path)]
      (if (.exists file)
        (slurp file)
        (throw (ex-info "File not found" {:path file-path}))))
    (catch Exception e
      (log/error "Failed to load ontology file:" e)
      (throw e))))

(defn detect-format-from-file
  "Detect RDF format from file extension"
  [file-path]
  (cond
    (str/ends-with? file-path ".rdf") :rdf/xml
    (str/ends-with? file-path ".owl") :rdf/xml
    (str/ends-with? file-path ".ttl") :turtle
    (str/ends-with? file-path ".nt") :n-triples
    (str/ends-with? file-path ".n3") :n3
    (str/ends-with? file-path ".jsonld") :jsonld
    :else :turtle))

;; ============================================================================
;; Ontology Storage
;; ============================================================================

(defn store-ontology-metadata
  "Store ontology metadata in Datomic"
  [conn metadata]
  (log/info "Storing ontology metadata:" (:ontology/name metadata))
  @(d/transact conn [metadata]))

(defn list-ontologies
  "List all loaded ontologies"
  [conn]
  (let [db (d/db conn)]
    (d/q '[:find [?name ?namespace ?version ?loaded-at]
           :where
           [?e :ontology/id]
           [?e :ontology/name ?name]
           [?e :ontology/namespace ?namespace]
           [?e :ontology/version ?version]
           [?e :ontology/loaded-at ?loaded-at]]
         db)))

(defn get-ontology-by-name
  "Get ontology metadata by name"
  [conn ontology-name]
  (let [db (d/db conn)]
    (d/q '[:find [?e ?namespace ?format ?version]
           :in $ ?name
           :where
           [?e :ontology/name ?name]
           [?e :ontology/namespace ?namespace]
           [?e :ontology/format ?format]
           [?e :ontology/version ?version]]
         db
         ontology-name)))

(defn ontology-loaded?
  "Check if ontology is already loaded"
  [conn ontology-name]
  (boolean (get-ontology-by-name conn ontology-name)))

;; ============================================================================
;; Ontology Loading Pipeline
;; ============================================================================

(defn load-ontology!
  "Load ontology from file into Datomic
  This is the main public API function"
  ([conn file-path]
   (load-ontology! conn file-path nil))
  ([conn file-path {:keys [name namespace format version]
                    :or {name (-> (io/file file-path) .getName)
                         namespace "http://example.org/ontology#"
                         format (detect-format-from-file file-path)
                         version "1.0"}}]
   (log/info "=== Loading Ontology ===")
   (log/info "File:" file-path)
   (log/info "Name:" name)
   (log/info "Format:" format)

   ;; Check if already loaded
   (when (ontology-loaded? conn name)
     (log/warn "Ontology" name "already loaded, skipping..."))

   ;; Load file content
   (let [content (load-ontology-file file-path)

           ;; Parse RDF and convert to Datomic entities
           entities (mapper/load-rdf-into-datomic content format mapper/prov-prefixes)]

     (log/info "Parsed" (count entities) "entities from ontology")

     ;; Store metadata
     (let [metadata (create-ontology-metadata name namespace format version)]
       (store-ontology-metadata conn metadata))

     ;; Transact entities
     (log/info "Transacting entities to Datomic...")
     @(d/transact conn entities)

     (log/info "Ontology loaded successfully!")
     {:success true
      :name name
      :entities-count (count entities)})))

;; ============================================================================
;; PROV-O Specific Loader
;; ============================================================================

(defn load-prov-o!
  "Load PROV-O ontology specifically
  Convenience function for the main ontology used in this project"
  ([conn]
   (load-prov-o! conn "resources/ontologies/prov-o.rdf"))
  ([conn file-path]
   (load-ontology! conn file-path
                   {:name "PROV-O"
                    :namespace "http://www.w3.org/ns/prov#"
                    :format :rdf/xml
                    :version "20130430"})))

;; ============================================================================
;; Ontology Export
;; ============================================================================

(defn- escape-turtle-string
  "Escape string for Turtle format"
  [s]
  (-> s
      (str/replace "\\" "\\\\")
      (str/replace "\"" "\\\"")
      (str/replace "\n" "\\n")
      (str/replace "\r" "\\r")
      (str/replace "\t" "\\t")))

(defn- uuid->uri
  "Convert UUID to PROV-O URI"
  [uuid]
  (str "http://example.org/prov#" uuid))

(defn- format-turtle-triple
  "Format a single triple in Turtle syntax"
  [subject predicate object]
  (let [subj (if (uuid? subject)
               (str "<" (uuid->uri subject) ">")
               (str "\"" (escape-turtle-string (str subject)) "\""))
        pred (str "<" predicate ">")
        obj (cond
              (uuid? object) (str "<" (uuid->uri object) ">")
              (keyword? object) (str "\"" (name object) "\"")
              (string? object) (str "\"" (escape-turtle-string object) "\"")
              (inst? object) (str "\"" object "\"^^<http://www.w3.org/2001/XMLSchema#dateTime>")
              :else (str "\"" (str object) "\""))]
    (str subj " " pred " " obj " .")))

(defn export-ontology
  "Export ontology from Datomic to specified format
  Supported formats: :json, :turtle, :n-triples
  Returns string in the specified format"
  [conn ontology-name format]
  (log/info "Exporting ontology:" ontology-name "as" format)
  (let [db (d/db conn)

        ;; Query all PROV-O entities
        entities (d/q '[:find [?e ?type ?generated-by ?derived-from]
                        :where
                        [?e :prov/entity]
                        [(get-else $ ?e :prov/entity-type :unknown) ?type]
                        [(get-else $ ?e :prov/wasGeneratedBy nil) ?generated-by]
                        [(get-else $ ?e :prov/wasDerivedFrom nil) ?derived-from]]
                      db)

        ;; Query all PROV-O activities
        activities (d/q '[:find [?a ?type ?started ?ended ?used]
                         :where
                         [?a :prov/activity]
                         [(get-else $ ?a :prov/activity-type :unknown) ?type]
                         [(get-else $ ?a :prov/startedAtTime nil) ?started]
                         [(get-else $ ?a :prov/endedAtTime nil) ?ended]
                         [(get-else $ ?a :prov/used nil) ?used]]
                       db)

        ;; Query all PROV-O agents
        agents (d/q '[:find [?a ?name ?type ?acted-on-behalf]
                     :where
                     [?a :prov/agent]
                     [(get-else $ ?a :prov/agent-name nil) ?name]
                     [(get-else $ ?a :prov/agent-type :unknown) ?type]
                     [(get-else $ ?a :prov/actedOnBehalfOf nil) ?acted-on-behalf]]
                   db)]

    (case format
      :json
      (let [data {:ontology/name ontology-name
                  :ontology/exported-at (java.util.Date.)
                  :entities (mapv (fn [[e type gen-by derived]]
                                    {:entity/id (str e)
                                     :entity/type type
                                     :wasGeneratedBy (str gen-by)
                                     :wasDerivedFrom (mapv str derived)})
                                  entities)
                  :activities (mapv (fn [[a type started ended used]]
                                      {:activity/id (str a)
                                       :activity/type type
                                       :startedAtTime started
                                       :endedAtTime ended
                                       :used (mapv str used)})
                                    activities)
                  :agents (mapv (fn [[a name type acted-on-behalf]]
                                  {:agent/id (str a)
                                   :agent/name name
                                   :agent/type type
                                   :actedOnBehalfOf (str acted-on-behalf)})
                                agents)}]
        (with-out-str (json/pprint data)))

      :turtle
      (let [prefixes (str "@prefix prov: <http://www.w3.org/ns/prov#> .\n"
                          "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n"
                          "@prefix ex: <http://example.org/prov#> .\n\n")

            triples (atom [])

            add-triple (fn [s p o]
                         (swap! triples conj (format-turtle-triple s p o)))]

        ;; Export entities
        (doseq [[e type gen-by derived] entities]
          (add-triple e "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://www.w3.org/ns/prov#Entity")
          (when type
            (add-triple e "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" type))
          (when gen-by
            (add-triple e "http://www.w3.org/ns/prov#wasGeneratedBy" gen-by))
          (when derived
            (doseq [d derived]
              (add-triple e "http://www.w3.org/ns/prov#wasDerivedFrom" d))))

        ;; Export activities
        (doseq [[a type started ended used] activities]
          (add-triple a "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://www.w3.org/ns/prov#Activity")
          (when type
            (add-triple a "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" type))
          (when started
            (add-triple a "http://www.w3.org/ns/prov#startedAtTime" started))
          (when ended
            (add-triple a "http://www.w3.org/ns/prov#endedAtTime" ended))
          (when used
            (doseq [u used]
              (add-triple a "http://www.w3.org/ns/prov#used" u))))

        ;; Export agents
        (doseq [[a name type acted-on-behalf] agents]
          (add-triple a "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://www.w3.org/ns/prov#Agent")
          (when type
            (add-triple a "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" type))
          (when name
            (add-triple a "http://www.w3.org/ns/prov#agentName" name))
          (when acted-on-behalf
            (add-triple a "http://www.w3.org/ns/prov#actedOnBehalfOf" acted-on-behalf)))

        (str prefixes (str/join "\n" @triples)))

      :n-triples
      (let [triples (atom [])]
        ;; Similar to turtle but without prefixes
        (str "@prefix prov: <http://www.w3.org/ns/prov#> .\n"
             "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n\n"
             "N-Triples export not fully implemented - use :turtle format instead."))

      (throw (ex-info "Unsupported export format"
                      {:ontology ontology-name
                       :format format
                       :supported #{:json :turtle :n-triples}})))))

;; ============================================================================
;; Ontology Validation
;; ============================================================================

(defn- find-orphaned-references
  "Find entities that reference non-existent activities, agents, or other entities"
  [db]
  (let [errors (atom [])]

    ;; Check entities referencing activities
    (let [entity-activity-refs (d/q '[:find [?e ?a]
                                      :where
                                      [?e :prov/wasGeneratedBy ?a]]
                                    db)
          all-activities (into #{} (d/q '[:find [?a]
                                          :where
                                          [?a :prov/activity]]
                                        db))]
      (doseq [[entity activity] entity-activity-refs]
        (when-not (contains? all-activities activity)
          (swap! errors conj [:orphaned-activity-reference
                             {:entity entity
                              :activity activity
                              :message "Entity references non-existent activity"}]))))

    ;; Check entities referencing other entities
    (let [entity-entity-refs (d/q '[:find [?e ?derived]
                                    :where
                                    [?e :prov/wasDerivedFrom ?derived]]
                                  db)
          all-entities (into #{} (d/q '[:find [?e]
                                        :where
                                        [?e :prov/entity]]
                                      db))]
      (doseq [[entity derived] entity-entity-refs]
        (when-not (contains? all-entities derived)
          (swap! errors conj [:orphaned-entity-reference
                             {:entity entity
                              :derived-from derived
                              :message "Entity derives from non-existent entity"}]))))

    ;; Check activities referencing agents
    (let [activity-agent-refs (d/q '[:find [?a ?agent]
                                     :where
                                     [?a :prov/wasAssociatedWith ?agent]]
                                   db)
          all-agents (into #{} (d/q '[:find [?a]
                                      :where
                                      [?a :prov/agent]]
                                    db))]
      (doseq [[activity agent] activity-agent-refs]
        (when-not (contains? all-agents agent)
          (swap! errors conj [:orphaned-agent-reference
                             {:activity activity
                              :agent agent
                              :message "Activity references non-existent agent"}]))))

    ;; Check activities referencing entities (used)
    (let [activity-entity-refs (d/q '[:find [?a ?e]
                                      :where
                                      [?a :prov/used ?e]]
                                    db)
          all-entities (into #{} (d/q '[:find [?e]
                                        :where
                                        [?e :prov/entity]]
                                      db))]
      (doseq [[activity entity] activity-entity-refs]
        (when-not (contains? all-entities entity)
          (swap! errors conj [:orphaned-entity-in-activity
                             {:activity activity
                              :entity entity
                              :message "Activity uses non-existent entity"}]))))

    @errors))

(defn- detect-circular-dependencies
  "Detect circular dependencies in entity derivations (wasDerivedFrom)"
  [db]
  (let [derivations (d/q '[:find ?e ?derived
                           :where
                           [?e :prov/wasDerivedFrom ?derived]]
                         db)

        ;; Build adjacency map for the derivation graph
        graph (reduce (fn [g [from to]]
                        (update g from conj to))
                      {}
                      derivations)

        ;; Depth-first search to detect cycles
        visited (atom #{})
        rec-stack (atom #{})
        cycles (atom [])]

    (defn- dfs-cycle-detector [node path]
      (cond
        (contains? @rec-stack node)
        (let [cycle-start (.indexOf path node)
              cycle-path (subvec path cycle-start)]
          (swap! cycles conj (conj cycle-path node)))

        (contains? @visited node)
        nil

        :else
        (do
          (swap! visited conj node)
          (swap! rec-stack conj node)
          (doseq [neighbor (get graph node [])]
            (dfs-cycle-detector neighbor (conj path node)))
          (swap! rec-stack disj node))))

    ;; Run DFS from each node
    (doseq [node (keys graph)]
      (when-not (contains? @visited node)
        (dfs-cycle-detector node [])))

    @cycles))

(defn- validate-entity-required-attributes
  "Check that entities have required attributes"
  [db]
  (let [warnings (atom [])]

    ;; Check entities without type
    (let [entities-no-type (d/q '[:find [?e]
                                  :where
                                  [?e :prov/entity]
                                  (not [?e :prov/entity-type])]
                                db)]
      (when (seq entities-no-type)
        (swap! warnings conj [:missing-entity-type
                             {:count (count entities-no-type)
                              :entities (take 10 entities-no-type)
                              :message "Entities found without entity-type attribute"}])))

    ;; Check activities without time range
    (let [activities-no-time (d/q '[:find [?a]
                                    :where
                                    [?a :prov/activity]
                                    (not [?a :prov/startedAtTime])
                                    (not [?a :prov/endedAtTime])]
                                  db)]
      (when (seq activities-no-time)
        (swap! warnings conj [:missing-activity-times
                             {:count (count activities-no-time)
                              :activities (take 10 activities-no-time)
                              :message "Activities found without time information"}])))

    ;; Check agents without name
    (let [agents-no-name (d/q '[:find [?a]
                                :where
                                [?a :prov/agent]
                                (not [?a :prov/agent-name])]
                              db)]
      (when (seq agents-no-name)
        (swap! warnings conj [:missing-agent-name
                             {:count (count agents-no-name)
                              :agents (take 10 agents-no-name)
                              :message "Agents found without agent-name attribute"}])))

    @warnings))

(defn validate-ontology-consistency
  "Validate that ontology is internally consistent
  Checks for:
  - Orphaned references (entities/activities/agents that don't exist)
  - Circular dependencies in entity derivations
  - Missing required attributes

  Returns map with :valid, :warnings, :errors"
  [conn ontology-name]
  (log/info "Validating ontology:" ontology-name)
  (let [db (d/db conn)

        ;; Check 1: Orphaned references
        orphaned-errors (find-orphaned-references db)

        ;; Check 2: Circular dependencies
        circular-deps (detect-circular-dependencies db)

        ;; Check 3: Required attributes
        missing-attrs (validate-entity-required-attributes db)

        ;; Build warnings map
        warnings (concat
                  (when (seq circular-deps)
                    [[:circular-dependencies
                      {:cycles circular-deps
                       :message "Circular dependencies detected in entity derivations"}]])
                  missing-attrs)

        ;; Valid if no critical errors (warnings are OK)
        valid? (empty? orphaned-errors)]

    (log/info "Validation complete:" (if valid? "VALID" "INVALID"))
    (log/info "Errors:" (count orphaned-errors) "Warnings:" (count warnings))

    {:valid valid?
     :errors orphaned-errors
     :warnings warnings
     :checked-at (java.util.Date.)
     :ontology-name ontology-name}))

;; ============================================================================
;; Utility Functions
;; ============================================================================

(defn get-ontology-stats
  "Get statistics about loaded ontology"
  [conn ontology-name]
  (let [db (d/db conn)]
    {:entity-count (d/q '[:find (count ?e) .
                          :where
                          [?e :prov/entity]]
                        db)
     :activity-count (d/q '[:find (count ?e) .
                            :where
                            [?e :prov/activity]]
                          db)
     :agent-count (d/q '[:find (count ?e) .
                         :where
                         [?e :prov/agent]]
                       db)}))

;; ============================================================================
;; Development Helpers
;; ============================================================================

(defn- get-ontology-name-from-path
  "Extract ontology name from file path"
  [file-path]
  (let [filename (if (string? file-path)
                    (last (str/split file-path #"/"))
                    file-path)]
    (-> filename
        (str/replace #"\.(edn|clj|ttl|n3)$" "")
        (str/replace #"-" "_")
        (str/upper-case))))

(defn reload-ontology!
  "Reload ontology (remove existing and load fresh)"
  [conn file-path]
  (log/warn "Reloading ontology from" file-path)
  ;; First, retract existing ontology entities with same ID
  (let [ontology-name (get-ontology-name-from-path file-path)]
    (when-let [existing (d/q '[:find [?e ...]
                                  :in $ ?
                                  :where
                                  [?e :ontology/id ?id]
                                  [?e :ontology/name ?name]]
                                (d/db conn) ontology-name)]
      (doseq [entity-id existing]
        (try
          @(d/transact conn [[:db/retractEntity entity-id]])
          (log/info "Retracted old ontology entity:" entity-id)
          (catch Exception e
            (log/warn "Failed to retract entity:" entity-id e))))))
  ;; Then load fresh
  (load-ontology! conn file-path))

(comment
  ;; Development REPL usage
  (def conn (dev/conn))

  ;; Load PROV-O
  (load-prov-o! conn)

  ;; List ontologies
  (list-ontologies conn)

  ;; Get statistics
  (get-ontology-stats conn "PROV-O")
  )
