(ns datomic-blockchain.data.turtle-loader
  "Simple Turtle (RDF/Turtle) parser for loading supply chain dataset

   This is a lightweight Turtle parser that extracts:
   - Prefix declarations
   - Triples (subject-predicate-object)
   - Entity/Activity/Agent declarations (PROV-O)

   For full RDF support, consider enabling RDF4J dependency."
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [taoensso.timbre :as log])
  (:import [java.util UUID Date]
           [java.text SimpleDateFormat]))

;; ============================================================================
;; Turtle Parser
;; ============================================================================

(defn trim-turtle-comment
  "Remove Turtle comments (# ...) from text"
  [text]
  (str/replace text #"\s*#.*$" ""))

(defn parse-turtle-string
  "Parse a quoted string in Turtle (e.g., \"value\" or '''value''')"
  [s]
  (or (some #(when (str/starts-with? s %)
                (subs s (count %) (- (count s) (count %))))
             ["\"\"\"" "\"" "'''" "'"])
      (str/trim s)))

(defn parse-turtle-number
  "Parse a numeric literal in Turtle (e.g., 123, 123.5, \"123\"^^xsd:integer)"
  [s datatype]
  (cond
    (= datatype "xsd:integer") (Long/parseLong s)
    (= datatype "xsd:long") (Long/parseLong s)
    (= datatype "xsd:float") (Float/parseFloat s)
    (= datatype "xsd:double") (Double/parseDouble s)
    (= datatype "xsd:boolean") (Boolean/parseBoolean s)
    (= datatype "xsd:dateTime") (java.sql.Timestamp/valueOf s)
    (= datatype "xsd:date") (.parse (java.text.SimpleDateFormat. "yyyy-MM-dd") s)
    (= datatype "xsd:time") (.parse (java.text.SimpleDateFormat. "HH:mm:ss") s)
    :else (try (Long/parseLong s)
               (catch Exception _ (try (Double/parseDouble s)
                                      (catch Exception _ s))))))

(defn parse-prefix
  "Parse a Turtle prefix declaration (e.g., @prefix ex: <http://example.org> .)"
  [line]
  (when (and (str/starts-with? line "@prefix")
             (str/includes? line ":")
             (str/includes? line "<"))
    (let [clean-line (str/replace line #"@prefix\s*" "")
          clean-line (str/replace clean-line #"\s*\.\s*$" "")
          [prefix-name rest] (str/split clean-line #":" 2)
          prefix-uri (when rest
                       (second (re-find #"<(.+?)>" rest)))]
      (when (and prefix-name prefix-uri)
        [(keyword (str/trim prefix-name)) prefix-uri]))))

(defn parse-triple
  "Parse a Turtle triple declaration (e.g., ex:subject ex:predicate \"object\" .)"
  [line prefixes]
  (when-let [[match subject-predicate object-part]
              (re-matches #"\s*(.+?)\s+(a\s+\w+|.+?)\s+(.+?)\s*\.\s*$" line)]
    (let [;; Parse subject (handle 'a' keyword)
          subject (if-let [[_ short-form] (re-matches #"a\s+(\S+)" (str/trim subject-predicate))]
                    (let [[_ prefix local] (re-matches #"(\w+):(\w+)" short-form)]
                      (keyword (str (get prefixes (keyword prefix)) local)))
                    subject-predicate)

          ;; Parse predicate
          predicate-raw (str/trim (nth match 0))
          predicate (cond
            ;; Handle 'a' (rdf:type)
            (= predicate-raw "a")
            :rdf/type

            ;; Handle prefixed name like prov:used
            (str/includes? predicate-raw ":")
            (let [[_ prefix local] (re-matches #"(\w+):(\w+)" predicate-raw)]
              (keyword (str (get prefixes (keyword prefix)) "/" local)))

            :else
            (keyword predicate-raw))

          ;; Parse object
          object-raw (str/trim object-part)
          object (cond
            ;; Boolean literal
            (= object-raw "true") true
            (= object-raw "false") false

            ;; Typed literal (e.g., "123"^^xsd:integer)
            (str/includes? object-raw "^^")
            (let [[_ value-str datatype] (str/split object-raw #"\"\^\^")]
              (parse-turtle-number (parse-turtle-string (str value-str "\"")) datatype))

            ;; String literal - check for quotes
            (or (str/starts-with? object-raw "\"") (str/starts-with? object-raw "'"))
            (parse-turtle-string object-raw)

            ;; Prefixed name
            (and (str/includes? object-raw ":")
                 (not (str/includes? object-raw "^^")))
            (let [[prefix local] (str/split object-raw #":" 2)]
              (keyword (str (get prefixes (keyword prefix)) "/" local)))

            ;; Number
            (re-matches #"-?\d+(\.\d+)?" object-raw)
            (parse-turtle-number object-raw nil)

            :else object-raw)]
      [subject predicate object])))

(defn parse-turtle-file
  "Parse a Turtle file into Clojure data structures

   Returns:
   {:prefixes {prefix => uri}
    :entities {subject => {attribute => value}}
    :activities {subject => {attribute => value}}
    :agents {subject => {attribute => value}}}"
  [file-path]
  (log/info "Loading Turtle file:" file-path)
  (let [lines (str/split-lines (slurp file-path))
        prefixes (atom {})
        triples (atom [])
        current-subject (atom nil)
        current-predicates (atom {})
        current-type (atom nil)]

    (doseq [line lines]
      (let [clean-line (str/trim (trim-turtle-comment line))]
        (cond
          ;; Empty line
          (str/blank? clean-line)
          nil

          ;; Prefix declaration
          (str/starts-with? clean-line "@prefix")
          (when-let [[prefix-name prefix-uri] (parse-prefix clean-line)]
            (swap! prefixes assoc prefix-name prefix-uri))

          ;; Subject declaration (e.g., ex:subject a prov:Entity ;)
          (and (str/ends-with? clean-line ";")
               (re-matches #"\s*(.+?)\s+a\s+(\w+):(\w+)\s*;" clean-line))
          (when-let [[_ subject type-prefix type-name]
                     (re-matches #"\s*(.+?)\s+a\s+(\w+):(\w+)\s*;" clean-line)]
            (do
              (when @current-subject
                (swap! triples conj [@current-subject @current-predicates @current-type])
                (reset! current-predicates {}))
              (reset! current-subject subject)
              (reset! current-type (keyword (str type-prefix ":" type-name)))))

          ;; Predicate declaration (e.g., ex:predicate "value" ;)
          (and (str/ends-with? clean-line ";")
               (not (str/starts-with? clean-line "@prefix")))
          (when-let [[subject predicate object]
                     (parse-triple (str/replace clean-line #";$" "") @prefixes)]
            (swap! current-predicates assoc predicate object))

          ;; Multi-line continuation - do nothing
          :else
          nil)))

    ;; Don't forget the last subject
    (when @current-subject
      (swap! triples conj [@current-subject @current-predicates @current-type]))

    ;; Organize by PROV-O types
    (let [prefixes @prefixes
          triples-data @triples
          result (reduce (fn [acc [subject predicates type]]
                          (-> acc
                              (update-in [:all] assoc subject predicates)
                              (update-in [:all subject :type] type)
                              ((fn [m]
                                 (cond
                                   (= type :prov/Entity)
                                   (update-in m [:entities] assoc subject predicates)
                                   (= type :prov/Activity)
                                   (update-in m [:activities] assoc subject predicates)
                                   (= type :prov/Agent)
                                   (update-in m [:agents] assoc subject predicates)
                                   :else
                                   (update-in m [:entities] assoc subject predicates))))))
                        {:prefixes prefixes
                         :entities {}
                         :activities {}
                         :agents {}
                         :all {}}
                        triples-data)]

      (log/info "Loaded" (count triples-data) "statements from" file-path)
      result)))

;; ============================================================================
;; Data Access Helpers
;; ============================================================================

(defn get-all-agents
  "Get all agents from loaded Turtle data"
  [turtle-data]
  (:agents turtle-data))

(defn get-all-entities
  "Get all entities from loaded Turtle data"
  [turtle-data]
  (:entities turtle-data))

(defn get-all-activities
  "Get all activities from loaded Turtle data"
  [turtle-data]
  (:activities turtle-data))

(defn find-by-batch-id
  "Find an entity by its batch ID"
  [turtle-data batch-id]
  (first (filter #(= (get % :uht/batch-id) batch-id)
                 (vals (:entities turtle-data)))))

(defn find-by-qr-code
  "Find an entity by its QR code"
  [turtle-data qr-code]
  (first (filter #(= (get % :uht/qr-code) qr-code)
                 (vals (:entities turtle-data)))))

(defn get-activities-for-entity
  "Get all activities that used or generated an entity"
  [turtle-data entity-id]
  (filter (fn [activity]
            (or (contains? (set (get activity :prov/used [])) entity-id)
                (= (get activity :prov/generated) entity-id)))
          (vals (:activities turtle-data))))

;; ============================================================================
;; Load All Dataset Files
;; ============================================================================

(defn load-uht-dataset
  "Load all UHT milk supply chain Turtle datasets from resources/datasets/uht-supply-chain/"
  []
  (log/info "Loading UHT Milk Supply Chain dataset...")
  (let [dataset-dir "resources/datasets/uht-supply-chain/"
        files ["participants.ttl" "products.ttl" "activities.ttl"]
        combined (atom {:prefixes {}
                       :entities {}
                       :activities {}
                       :agents {}
                       :all {}})]

    (doseq [file files
            :let [path (str dataset-dir file)]
            :when (.exists (io/file path))]
      (let [data (parse-turtle-file path)]
        ;; Merge prefixes
        (swap! combined update :prefixes merge (:prefixes data))
        ;; Merge entities
        (swap! combined update :entities merge (:entities data))
        ;; Merge activities
        (swap! combined update :activities merge (:activities data))
        ;; Merge agents
        (swap! combined update :agents merge (:agents data))
        ;; Merge all
        (swap! combined update :all merge (:all data))))

    (log/info "Dataset loaded successfully:"
              (count (:entities @combined)) "entities,"
              (count (:activities @combined)) "activities,"
              (count (:agents @combined)) "agents")
    @combined))

;; ============================================================================
;; Convert to Blockchain Transaction Format
;; ============================================================================

(defn entity->transaction-data
  "Convert a Turtle entity to blockchain transaction data format"
  [entity-data]
  (-> entity-data
      (select-keys [:uht/batch-id :uht/product :uht/origin :uht/volume-liters
                    :uht/fat-content :uht/protein-content :uht/temperature-at-collection
                    :uht/quality-grade :uht/batch-id :uht/qr-code :uht/variant
                    :uht:variant-name :uht:price-amount])
      (clojure.set/rename-keys {:uht/batch-id :traceability/batch
                                :uht/product :traceability/product
                                :uht/origin :traceability/origin
                                :uht/qr-code :traceability/qr-code})))

(defn activity->transaction-data
  "Convert a Turtle activity to blockchain transaction data format"
  [activity-data]
  (-> activity-data
      (select-keys [:prov:startedAtTime :prov:endedAtTime :uht:temperature-min
                    :uht:temperature-max :uht:cold-chain-maintained :uht:truck-id])
      (clojure.set/rename-keys {:prov:startedAtTime :traceability/start-time
                                :prov:endedAtTime :traceability/end-time})))

(comment
  ;; Usage examples
  (def data (load-uht-dataset))

  ;; Get all agents
  (get-all-agents data)

  ;; Find product by batch ID
  (find-by-batch-id data "MILK-THAI-2024-001")

  ;; Find product by QR code
  (find-by-qr-code data "UHT-CHOC-2024-001-QR")

  ;; Get activities for an entity
  (get-activities-for-entity data :http/example.org/uht-milk#entity-milk-batch-001)
  )
