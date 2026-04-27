(ns datomic-blockchain.ontology.blank-node-support
  "Blank node handling for RDF/PROV-O ontology

  Blank nodes (identified as _:identifier in RDF) pose a problem for blockchain
  systems because:
  1. They are scoped to a single RDF document
  2. Different documents may use the same blank node identifier
  3. Hashing blank node identifiers directly causes collisions

  This module provides safe blank node handling by:
  - Detecting blank nodes in RDF input
  - Replacing blank nodes with UUIDs (globally unique)
  - Maintaining blank node equivalence within a document
  - Using cryptographic hashing for deterministic mapping"
  (:require [clojure.string :as str]
            [clojure.uuid :as uuid]
            [taoensso.timbre :as log]))

;; ============================================================================
;; Blank Node Detection
;; ============================================================================

(def blank-node-pattern
  "Regex pattern to match RDF blank nodes
  Matches:
  - _:b1, _:auto123, _:gen1 (standard blank node identifiers)
  - _: (minimal blank node)"
  #"_:[a-zA-Z0-9_-]*")

(defn blank-node?
  "Check if a string is a blank node identifier

  Parameters:
    s: String to check

  Returns:
    true if s is a blank node identifier (e.g., '_:b1'), false otherwise"
  [s]
  (boolean (and (string? s)
                (re-matches blank-node-pattern s))))

(defn find-blank-nodes
  "Find all blank node identifiers in RDF data

  Parameters:
    rdf-string: String containing RDF data (Turtle/N-Triples format)

  Returns:
    Set of unique blank node identifiers found"
  [rdf-string]
  (when (string? rdf-string)
    (let [matches (re-seq blank-node-pattern rdf-string)]
      (set matches))))

;; ============================================================================
;; Blank Node to UUID Mapping
;; ============================================================================

(defn generate-deterministic-uuid
  "Generate a deterministic UUID from a blank node identifier and document context

  Uses SHA-256 hash of:
  - Blank node identifier (e.g., 'b1' from '_:b1')
  - Document context (RDF content hash or provided seed)
  - Namespace UUID (provenance ontology namespace)

  This ensures:
  - Same blank node in same document → same UUID (deterministic)
  - Same blank node in different documents → different UUIDs (collision-free)
  - Different blank nodes → different UUIDs

  Parameters:
    blank-id: Blank node identifier (e.g., 'b1' from '_:b1')
    context-seed: Document-specific context (string or bytes)

  Returns:
    UUID object"
  [blank-id context-seed]
  (try
    ;; Get the local part (remove '_:' prefix)
    (let [local-part (str/replace blank-id #"^_:" "")
          ;; Create combined input for hashing
          combined (str "blank-node:" local-part ":" context-seed)
          ;; Use Java's UUID nameUUIDFromBytes (version 3, name-based)
          ;; This uses MD5 which is deterministic and sufficient for our purpose
          md (java.security.MessageDigest/getInstance "MD5")
          hash-bytes (.digest md (.getBytes combined "UTF-8"))]
      ;; Convert to UUID using name-based approach (version 3)
      ;; nameUUIDFromBytes expects a byte array of 16 bytes
      (java.util.UUID/nameUUIDFromBytes hash-bytes))
    (catch Exception e
      (log/error "Failed to generate UUID for blank node:" blank-id e)
      ;; Fallback: generate random UUID
      (random-uuid))))

(defn create-blank-node-mapping
  "Create a mapping from blank node identifiers to UUIDs

  Each blank node in the RDF document gets a unique UUID.
  The same blank node identifier always maps to the same UUID within
  the context of a single document.

  Parameters:
    blank-nodes: Set of blank node identifiers (e.g., '#{'_:b1' '_:b2'})
    context-seed: Document-specific context for deterministic mapping

  Returns:
    Map of blank-node-id → UUID"
  [blank-nodes context-seed]
  (log/debug "Creating blank node mapping for" (count blank-nodes) "blank nodes")
  (into {}
        (map (fn [blank-id]
               [blank-id (generate-deterministic-uuid blank-id context-seed)]))
        blank-nodes))

;; ============================================================================
;; RDF Blank Node Replacement
;; ============================================================================

(defn replace-blank-nodes
  "Replace blank node identifiers in RDF string with UUIDs

  Parameters:
    rdf-string: String containing RDF data
    blank-node-map: Map of blank-node-id → UUID from create-blank-node-mapping

  Returns:
    RDF string with blank nodes replaced by their UUID equivalents

  Example:
    _:b1 prov:wasGeneratedBy _:b2
    →
    #uuid \\\"...\\\" prov:wasGeneratedBy #uuid \\\"...\\\"\"
  "
  [rdf-string blank-node-map]
  (reduce (fn [rdf [blank-id uuid]]
            (str/replace rdf (re-pattern (java.util.regex.Pattern/quote blank-id))
                        (str "#uuid \"" (str uuid) "\"")))
          rdf-string
          blank-node-map))

(defn replace-blank-nodes-in-triples
  "Replace blank nodes in parsed triple data structure

  Parameters:
    triples: Sequence of [subject predicate object] triples
    blank-node-map: Map of blank-node-id → UUID

  Returns:
    Sequence of triples with blank nodes replaced by UUIDs"
  [triples blank-node-map]
  (map (fn [[subject predicate object]]
         (let [replace-fn (fn [node]
                            (if (blank-node? node)
                              (get blank-node-map node node)
                              node))]
           [(replace-fn subject)
            predicate
            (replace-fn object)]))
       triples))

;; ============================================================================
;; Document Context Generation
;; ============================================================================

(defn generate-document-context
  "Generate a deterministic context seed for a document

  The context seed ensures that blank nodes from different documents
  don't collide while maintaining consistency within a document.

  Parameters:
    rdf-content: The RDF document content
    use-hash: If true, use SHA-256 hash of content (default: true)

  Returns:
    String context seed for blank node mapping"
  ([rdf-content]
   (generate-document-context rdf-content true))
  ([rdf-content use-hash]
   (try
     (if use-hash
       (let [md (java.security.MessageDigest/getInstance "SHA-256")
             hash-bytes (.digest md (.getBytes rdf-content "UTF-8"))
             ;; hex-bytes (javax.xml.bind.DatatypeConverter/printHexBinary hash-bytes)
             hex-str (apply str (map (fn [b]
                                       (format "%02x" (bit-and 0xFF b)))
                                     hash-bytes))]
         (str "doc:" (subs hex-str 0 16)))  ; Use first 16 chars of hash
       ;; Alternative: use content length and first 100 chars
       (str "doc:" (hash rdf-content) "-" (min 100 (count rdf-content))))
     (catch Exception e
       (log/warn "Failed to generate document context, using fallback" e)
       (str "doc:fallback-" (System/currentTimeMillis))))))

;; ============================================================================
;; Complete Blank Node Processing Pipeline
;; ============================================================================

(defn process-rdf-with-blank-nodes
  "Process RDF data to safely handle blank nodes

  Complete pipeline:
  1. Detect blank nodes in RDF input
  2. Generate document context
  3. Create blank node → UUID mapping
  4. Replace blank nodes with UUIDs
  5. Return processed RDF and mapping

  Parameters:
    rdf-content: String containing RDF data
    opts: Options map with keys:
      - :context-seed - Override document context (optional)
      - :preserve-mapping - Return blank node mapping (default: true)

  Returns:
    Map with:
    - :processed-rdf - RDF string with blank nodes replaced
    - :blank-node-map - Map of blank-node-id → UUID
    - :blank-nodes-found - Set of blank node identifiers found
    - :blank-node-count - Number of blank nodes processed"
  ([rdf-content]
   (process-rdf-with-blank-nodes rdf-content {}))
  ([rdf-content opts]
   (log/debug "Processing RDF for blank nodes")
   (let [blank-nodes (find-blank-nodes rdf-content)
         context-seed (or (:context-seed opts)
                          (generate-document-context rdf-content))
         blank-node-map (create-blank-node-mapping blank-nodes context-seed)
         processed-rdf (replace-blank-nodes rdf-content blank-node-map)]
     {:processed-rdf processed-rdf
      :blank-node-map blank-node-map
      :blank-nodes-found blank-nodes
      :blank-node-count (count blank-nodes)
      :context-seed context-seed})))

(defn process-rdf-safe
  "Safely process RDF, handling blank nodes or passing through if none found

  Parameters:
    rdf-content: String containing RDF data

  Returns:
    Processed RDF string (blank nodes replaced) or original if no blank nodes"
  [rdf-content]
  (if-let [blank-nodes (seq (find-blank-nodes rdf-content))]
    (do
      (log/info "Found" (count blank-nodes) "blank nodes, processing...")
      (:processed-rdf (process-rdf-with-blank-nodes rdf-content)))
    (do
      (log/debug "No blank nodes found in RDF")
      rdf-content)))

;; ============================================================================
;; Validation
;; ============================================================================

(defn validate-no-blank-nodes
  "Validate that RDF content contains no blank nodes

  Parameters:
    rdf-content: String containing RDF data

  Returns:
    ValidationResult map with :valid? key"
  [rdf-content]
  (let [blank-nodes (find-blank-nodes rdf-content)]
    (if (empty? blank-nodes)
      {:valid? true
       :message "No blank nodes found"}
      {:valid? false
       :message (str "Blank nodes found: " (clojure.string/join ", " blank-nodes))
       :blank-nodes blank-nodes
       :count (count blank-nodes)})))

;; ============================================================================
;; Utilities
;; ============================================================================

(defn blank-node-statistics
  "Generate statistics about blank nodes in RDF content

  Parameters:
    rdf-content: String containing RDF data

  Returns:
    Map with statistics:
    - :total-blank-nodes - Total count
    - :unique-blank-nodes - Count of unique identifiers
    - :blank-nodes - Set of unique blank node identifiers"
  [rdf-content]
  (let [blank-nodes (find-blank-nodes rdf-content)
        all-occurrences (re-seq blank-node-pattern rdf-content)]
    {:total-blank-nodes (count all-occurrences)
     :unique-blank-nodes (count blank-nodes)
     :blank-nodes blank-nodes
     :blank-node-frequency (frequencies all-occurrences)}))

(comment
  ;; Development REPL usage

  ;; Test blank node detection
  (blank-node? "_:b1")  ; => true
  (blank-node? "http://example.org")  ; => false

  ;; Test finding blank nodes
  (find-blank-nodes "_:b1 prov:wasGeneratedBy _:b2")  ; => #{"_:b1" "_:b2"}

  ;; Test processing
  (process-rdf-with-blank-nodes "_:b1 prov:type _:b2")

  ;; Validate
  (validate-no-blank-nodes "_:b1 prov:type _:b2")  ; => {:valid? false ...}
  (validate-no-blank-nodes "<ex:thing> prov:type <ex:Entity>")  ; => {:valid? true ...}
  )
