(ns datomic-blockchain.ontology.mapper
  "RDF to Datomic mapping for ontology integration

  Implements basic RDF (Turtle/N-Triples) parsing without external dependencies.
  For production use with complex RDF, consider integrating RDF4J or Apache Jena.

  Includes blank node handling to prevent hash collisions in blockchain."
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [datomic.api :as d]
            [datomic-blockchain.ontology.blank-node-support :as blank-node]
            [taoensso.timbre :as log]))

;; ============================================================================
;; Prefix Management
;; ============================================================================

(def prov-prefixes
  "PROV-O ontology prefixes"
  {"prov" "http://www.w3.org/ns/prov#"
   "xsd" "http://www.w3.org/2001/XMLSchema#"
   "rdf" "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
   "rdfs" "http://www.w3.org/2000/01/rdf-schema#"
   "owl" "http://www.w3.org/2002/07/owl#"
   "ex" "http://example.org/"})

(defn expand-prefix
  "Expand a prefixed name (e.g., prov:Entity) to full URI"
  [prefixed-name prefixes]
  (when (string? prefixed-name)
    (if (str/starts-with? prefixed-name "http:")
      prefixed-name
      (when-let [[_ prefix local] (re-matches #"([^:]+):(.+)" prefixed-name)]
        (str (get prefixes prefix prefix) local)))))

(defn expand-curie
  "Compact URI expansion for RDF mapping"
  [curie]
  (expand-prefix curie prov-prefixes))

;; ============================================================================
;; Simple Turtle/N-Triples Parser
;; ============================================================================

(defn parse-turtle-line
  "Parse a single Turtle/N-Triples line into [subject predicate object]
  Enhanced to handle UUID literals from blank node processing"
  [line]
  (try
    (let [trimmed (str/trim line)]
      (when (and (not-empty trimmed)
                 (not (str/starts-with? trimmed "#"))
                 (not (str/starts-with? trimmed "@")))
        ;; Enhanced regex to match URIs, UUIDs, and literals
        (let [uri-re #"<([^>]+)>"
              uuid-re #"#uuid \"([a-f0-9-]+)\""
              literal-re #"\"([^\"]*)\"(?:@([a-z]+)|\^\^<([^>]+)>)?"
              ;; Try to match UUID first, then URIs, then literals
              uuid-match (re-find uuid-re trimmed)
              parts (if uuid-match
                      ;; Handle UUID from blank node processing
                      (concat [[(first uuid-match) (second uuid-match)]]
                              (re-seq (str uri-re "|" literal-re)
                                     (str/replace trimmed (re-pattern (str "\\Q" (first uuid-match) "\\E")) "")))
                      (re-seq (str uri-re "|" literal-re) trimmed))]
          (when (>= (count parts) 3)
            (let [subject (some #(when (< (count %) 3) (second %)) parts)
                  predicate (second (nth parts 1))
                  object (let [o (nth parts 2)]
                           (cond
                             ;; UUID object (from blank node)
                             (and (string? o) (re-matches #"[a-f0-9-]{36}" o))
                             o
                             ;; URI object
                             (str/starts-with? (first o) "<")
                             (second o)
                             ;; Literal object
                             :else
                             (second o)))]
              [subject predicate object])))))
    (catch Exception e
      (log/debug "Failed to parse line:" line e)
      nil)))

(defn parse-rdf-turtle
  "Parse simple Turtle/N-Triples format RDF data
  Returns sequence of [subject predicate object] triples

  Automatically handles blank nodes by replacing them with UUIDs
  to prevent hash collisions in blockchain storage."
  [rdf-string]
  ;; Process blank nodes before parsing
  (let [processed-rdf (blank-node/process-rdf-safe rdf-string)
        lines (str/split-lines processed-rdf)]
    (keep parse-turtle-line lines)))

;; ============================================================================
;; RDF to Datomic Conversion
;; ============================================================================

(defn rdf-type->datomic-type
  "Map RDF XSD types to Datomic value types"
  [xsd-type]
  (cond
    (str/ends-with? xsd-type "string") :db.type/string
    (str/ends-with? xsd-type "integer") :db.type/long
    (str/ends-with? xsd-type "boolean") :db.type/boolean
    (str/ends-with? xsd-type "dateTime") :db.type/instant
    (str/ends-with? xsd-type "double") :db.type/double
    (str/ends-with? xsd-type "float") :db.type/float
    :else :db.type/string))

(defn rdf-triple->datomic
  "Convert an RDF triple to Datomic transaction format
  Returns a map suitable for transacting

  Handles:
  - UUID subjects (from blank node processing)
  - URI subjects
  - Literal objects"
  [[subject predicate object] prefixes]
  (let [full-pred (expand-prefix predicate prefixes)
        ;; Generate consistent tempid for subject
        ;; Use UUID if subject is a UUID, otherwise hash
        subject-temp (if (and (string? subject)
                             (re-matches #"[a-f0-9-]{36}" subject))
                       (str "temp-" subject)
                       (str "temp-" (hash subject)))]
    (cond
      ;; rdf:type - maps to entity type
      (str/ends-with? full-pred "type")
      {subject-temp :prov/entity-type
       :prov/type-value (expand-prefix object prefixes)}

      ;; prov:wasGeneratedBy - activity reference (if object is UUID)
      (and (str/ends-with? full-pred "wasGeneratedBy")
           (string? object)
           (re-matches #"[a-f0-9-]{36}" object))
      {(keyword subject-temp) :prov/wasGeneratedBy
       :prov/generated-by-id (java.util.UUID/fromString object)}

      ;; prov:used - entity reference (if object is UUID)
      (and (str/ends-with? full-pred "used")
           (string? object)
           (re-matches #"[a-f0-9-]{36}" object))
      {(keyword subject-temp) :prov/used
       :prov/used-id (java.util.UUID/fromString object)}

      ;; prov:wasAssociatedWith - agent reference
      (and (str/ends-with? full-pred "wasAssociatedWith")
           (string? object)
           (re-matches #"[a-f0-9-]{36}" object))
      {(keyword subject-temp) :prov/wasAssociatedWith
       :prov/associated-id (java.util.UUID/fromString object)}

      ;; Default: store as generic attribute
      :else
      {(keyword subject-temp) (keyword (str/replace full-pred #"[^a-zA-Z0-9_-]" "_"))
       :attr-value object})))

(defn rdf->datomic
  "Convert RDF data to Datomic transactions
  Supports basic Turtle/N-Triples format with blank node handling

  Parameters:
    rdf-data: String containing RDF in Turtle/N-Triples format
    format: :turtle or :n-triples (currently same implementation)
    opts: Options map:
      - :process-blank-nodes - Enable blank node processing (default: true)

  Returns: Vector of Datomic transaction maps"
  ([rdf-data format]
   (rdf->datomic rdf-data format {}))
  ([rdf-data format opts]
   (try
     ;; Process blank nodes if enabled (default)
     (let [process-blank? (get opts :process-blank-nodes true)
           processed-rdf (if process-blank?
                           (blank-node/process-rdf-safe rdf-data)
                           rdf-data)
           blank-info (when process-blank?
                        (blank-node/process-rdf-with-blank-nodes rdf-data))
           triples (parse-rdf-turtle processed-rdf)
           entities (->> triples
                        (map #(rdf-triple->datomic % prov-prefixes))
                        (group-by :db/id)
                        (map (fn [[_ es]] (apply merge es))))
           ;; Assign proper tempids (sequential, not hash-based)
           indexed (map-indexed (fn [i e] (assoc e :db/id (str "temp-" i))) entities)
           result (vec indexed)]
       ;; Log blank node processing info
       (when (and process-blank? blank-info)
         (log/info "RDF processing:" (:blank-node-count blank-info) "blank nodes replaced"))
       result)
     (catch Exception e
       (log/error "Failed to parse RDF:" e)
       {:error (str "Failed to parse RDF: " (.getMessage e))
       :data rdf-data}))))

(defn load-rdf
  "Load RDF from file or string
  Returns parsed triples"
  [source]
  (try
    (cond
      ;; File path
      (and (string? source)
           (or (.exists (io/file source))
               (str/starts-with? source "resources/")
               (str/starts-with? source "/")))
      (let [content (slurp source)]
        (parse-rdf-turtle content))

      ;; Direct RDF string
      (string? source)
      (parse-rdf-turtle source)

      :else
      {:error "Invalid RDF source"})
    (catch Exception e
      {:error (str "Failed to load RDF: " (.getMessage e))})))

(defn load-rdf-into-datomic
  "Load RDF data into Datomic database
  Parses RDF and transacts entities to Datomic

  Parameters:
    conn: Datomic connection
    source: File path or RDF string
    format: :turtle or :n-triples
    prefixes: Optional map of prefix -> URI mappings

  Returns: Transaction result"
  ([conn source format]
   (load-rdf-into-datomic conn source format prov-prefixes))
  ([conn source format prefixes]
   (try
     (let [triples (if (map? source)
                     (:error source)  ; Already an error
                     (load-rdf source))
           tx-data (rdf->datomic (if (map? triples)
                                  ""
                                  (str/join "\n" (map pr-str triples)))
                                format)]
       (if (map? tx-data)
         {:error (:error tx-data)}
         @(d/transact conn tx-data)))
     (catch Exception e
       {:error (str "Failed to load RDF into Datomic: " (.getMessage e))}))))

;; ============================================================================
;; Datomic to RDF Conversion
;; ============================================================================

(defn datomic-entity->rdf
  "Convert a Datomic entity to RDF triple format"
  [db entity-id format]
  (try
    (let [entity (d/entity db entity-id)
          attrs (keys entity)
          prov-attrs (filter #(str/starts-with? (name %) "prov/") attrs)]
      (vec
       (for [attr prov-attrs]
         (let [val (attr entity)
               subj (str "<" (str entity-id) ">")
               pred (str "<" (str/replace (name attr) "-" "/") ">")
               obj (cond
                      (uuid? val) (str "<" val ">")
                      (keyword? val) (str "\"" (name val) "\"")
                      (inst? val) (str "\"" val "\"^^<http://www.w3.org/2001/XMLSchema#dateTime>")
                      :else (str "\"" val "\""))]
           [subj pred obj]))))
    (catch Exception e
      {:error (str "Failed to convert entity: " (.getMessage e))})))

(defn datomic->rdf
  "Convert Datomic data to RDF format
  Returns string in Turtle/N-Triples format"
  [db entity-id format]
  (let [triples (datomic-entity->rdf db entity-id format)]
    (if (map? triples)
      triples
      (str/join "\n"
                (map (fn [[s p o]] (str s " " p " " o " ."))
                     triples)))))

(defn export-entities-as-turtle
  "Export all entities of a given type as Turtle RDF
  Useful for ontology export"
  [db entity-type]
  (let [entities (d/q '[:find [?e ...]
                        :in $ ?type
                        :where
                        [?e :prov/entity-type ?type]]
                      db
                      entity-type)]
    (str/join "\n\n"
              (map #(datomic->rdf db % :turtle)
                   entities))))