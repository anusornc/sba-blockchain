(ns datomic-blockchain.api.handlers.query
  "Safe Datomic query handlers with whitelist validation"
  (:require [clojure.walk :as walk]
            [taoensso.timbre :as log]
            [datomic.api :as d]
            [datomic-blockchain.api.handlers.common :as common]
            [clojure.data.json :as json]))

;; ============================================================================
;; Query Whitelist (SECURITY: Prevent arbitrary Datomic queries)
;; ============================================================================

(def ^:private allowed-queries
  "Whitelist of pre-approved safe query templates.
   Each entry defines a query pattern with required structure."
  {:get-entity-by-id
   {:description "Get entity by its database ID"
    :template [:find '?e :where ['?e :db/id '?id]]
    :required-params [:id]
    :allowed-attrs [:db/id]}

   :get-entity-by-uuid
   {:description "Get entity by UUID"
    :template [:find '?e :where ['?e :db/ident '?ident]]
    :required-params [:ident]
    :allowed-attrs [:db/ident]}

   :get-prov-entities
   {:description "Get PROV-O entities"
    :template [:find '?e :where ['?e :prov/entity '?entity-id]]
    :required-params [:entity-id]
    :allowed-attrs [:prov/entity]}

   :get-prov-activities
   {:description "Get PROV-O activities"
    :template [:find '?a :where ['?a :prov/activity '?activity-id]]
    :required-params [:activity-id]
    :allowed-attrs [:prov/activity]}

   :get-prov-agents
   {:description "Get PROV-O agents"
    :template [:find '?ag :where ['?ag :prov/agent '?agent-id]]
    :required-params [:agent-id]
    :allowed-attrs [:prov/agent]}

   :get-traceability-products
   {:description "Get products by batch ID"
    :template [:find '?p :where ['?p :traceability/batch '?batch]]
    :required-params [:batch]
    :allowed-attrs [:traceability/batch]}

   :pull-entity-by-id
   {:description "Pull entity with all attributes by ID"
    :template [:find '(pull '?e '*) :where ['?e :db/id '?id]]
    :required-params [:id]
    :allowed-attrs [:db/id]}

   :count-entities
   {:description "Count total entities"
    :template [:find '(count '?e) :where ['?e :prov/entity]]
    :required-params []
    :allowed-attrs [:prov/entity]}

   :count-activities
   {:description "Count total activities"
    :template [:find '(count '?a) :where ['?a :prov/activity]]
    :required-params []
    :allowed-attrs [:prov/activity]}})

;; ============================================================================
;; Query Validation
;; ============================================================================

(defn- normalize-query-for-comparison
  "Normalize query structure for whitelist comparison."
  [query]
  (let [vector-query (if (map? query)
                       (vec (mapcat identity query))
                       query)]
    (walk/postwalk
     (fn [x]
       (cond
         (and (symbol? x) (.startsWith (str x) "?"))
         (keyword (str x))
         (symbol? x)
         (keyword x)
         :else
         x))
     vector-query)))

(defn- match-template?
  "Check if query matches a template pattern."
  [query template]
  (try
    (let [query-find-index (.indexOf query :find)
          template-find-index (.indexOf template :find)
          query-find (when (>= query-find-index 0)
                       (and (< (inc query-find-index) (count query))
                            (nth query (inc query-find-index))))
          template-find (when (>= template-find-index 0)
                          (and (< (inc template-find-index) (count template))
                               (nth template (inc template-find-index))))
          has-find (and (>= query-find-index 0) (>= template-find-index 0))
          query-attrs (->> (tree-seq coll? seq query)
                           (filter keyword?)
                           (filter namespace)
                           set)
          template-attrs (->> (tree-seq coll? seq template)
                              (filter keyword?)
                              (filter namespace)
                              set)]

      (and has-find
           query-find
           template-find
           (= query-attrs template-attrs)
           (or (and (vector? query-find) (vector? template-find))
               (and (keyword? query-find) (keyword? template-find))
               (and (list? query-find) (list? template-find))
               (and (symbol? query-find) (symbol? template-find))
               (and (symbol? template-find)
                    (or (symbol? query-find)
                        (keyword? query-find))))
           (contains? (set query) :where)
           (contains? (set template) :where)))
    (catch Exception e
      (log/error "Template matching error:" (.getMessage e))
      false)))

(defn query-allowed?
  "Check if query matches any allowed query template.
   Returns {:allowed true/false :matched-template id-or-nil}"
  [query]
  (try
    (let [normalized (normalize-query-for-comparison query)]
      (reduce
       (fn [_ [template-id template-data]]
         (let [template (:template template-data)
               normalized-template (normalize-query-for-comparison template)]
           (when (match-template? normalized normalized-template)
             (reduced {:allowed true
                       :matched-template template-id
                       :template template-data}))))
       {:allowed false :matched-template nil}
       allowed-queries))
    (catch Exception e
      (log/error "Query validation error:" (.getMessage e))
      {:allowed false :matched-template nil :error (.getMessage e)})))

;; ============================================================================
;; Query Execution
;; ============================================================================

(defn- symbolize-query
  "Convert string representations in query to symbols/keywords"
  [query]
  (walk/postwalk
   (fn [x]
     (cond
       (and (string? x) (contains? #{"find" "where" "in" "with"} x))
       (keyword x)
       (and (string? x) (.startsWith x "?"))
       (symbol x)
       (and (string? x)
            (or (.startsWith x ":")
                (.contains x "/")))
       (let [kw-str (if (.startsWith x ":") (subs x 1) x)]
         (keyword kw-str))
       :else
       x))
   query))

(def ^:private attr-value-coercers
  "Attribute-specific coercion for literal query values.
   This keeps benchmarked whitelist queries semantically correct when values
   arrive as JSON strings over HTTP."
  {:prov/entity common/parse-uuid-safe
   :prov/activity common/parse-uuid-safe
   :prov/agent common/parse-uuid-safe
   :db/id parse-long})

(defn- coerce-clause-literal
  "Coerce the literal portion of a simple datalog clause when an attribute
   expects a typed value such as UUID."
  [clause]
  (if (and (vector? clause)
           (= 3 (count clause))
           (keyword? (second clause)))
    (let [[entity attr value] clause
          coerce-fn (get attr-value-coercers attr)]
      (if (and coerce-fn (not (symbol? value)))
        (if-let [coerced (coerce-fn value)]
          [entity attr coerced]
          clause)
        clause))
    clause))

(defn- coerce-query-literals
  "Walk the executable query and coerce supported literal values before
   Datomic execution."
  [query]
  (walk/postwalk coerce-clause-literal query))

(defn- valid-query-structure?
  "Check if query has required structure (:find and :where keys)"
  [query]
  (or
   (and (map? query)
        (contains? query :find)
        (contains? query :where))
   (and (vector? query)
        (contains? (set query) :find)
        (contains? (set query) :where))))

(defn- map-query-to-vector
  "Convert map format query to vector format for Datomic execution."
  [query]
  (if (map? query)
    (let [find-clause (:find query)
          where-clauses (get query :where)
          other-clauses (dissoc query :find :where)]
      (vec (concat
            [:find find-clause]
            (when where-clauses
              [:where (apply vector (mapcat identity where-clauses))])
            (mapcat identity other-clauses))))
    query))

(defn- format-query-response
  "Format query for response"
  [query]
  (if (map? query)
    (select-keys query [:find :where])
    {:find (when (contains? (set query) :find)
             (second query))
     :where (when (contains? (set query) :where)
              (first (filter vector? (drop-while #(not= :where %) query))))}))

(defn- extract-query
  "Extract query from request, checking all possible locations"
  [request]
  (letfn [(read-query-key [m]
            (when (map? m)
              (or (get m :query)
                  (get m "query"))))]
  (or
   (read-query-key (:body-params request))
   (read-query-key (:params request))
   (read-query-key (:body request))
   (when (string? (:body request))
     (try
       (read-query-key (json/read-str (:body request)))
       (catch Exception _ nil))))))

(defn handle-query
  "Execute safe Datomic query from JSON input

   SECURITY ENHANCEMENTS:
   1. Query whitelist - only pre-approved query templates allowed
   2. Structure validation - must be a map with :find and :where keys
   3. No arbitrary code execution - queries are data structures, not code"
  [request connection]
  (let [raw-query (extract-query request)
        query-params (when raw-query (symbolize-query raw-query))]

    (cond
      ;; Missing query
      (nil? query-params)
      (common/validation-error "Missing query parameter" {:param :query})

      ;; Query not in whitelist
      (not (:allowed (query-allowed? query-params)))
      (let [whitelist-check (query-allowed? query-params)]
        (log/warn "Query not in whitelist blocked:"
                  (:matched-template whitelist-check)
                  "Query:" query-params)
        (common/error
         (str "Query not allowed. Only pre-approved query templates are permitted.")
         403
         {:matched-template (:matched-template whitelist-check)}))

      ;; Invalid query structure
      (not (valid-query-structure? query-params))
      (common/validation-error "Invalid query format" {:required [:find :where]})

      ;; Execute validated query
      :else
      (try
        (let [db (d/db connection)
              executable-query (-> query-params
                                   map-query-to-vector
                                   coerce-query-literals)
              results (d/q executable-query db)
              whitelist-check (query-allowed? query-params)]
          (log/info "Query executed successfully:"
                    (:matched-template whitelist-check)
                    "Results:" (count results))
          (common/success
           {:query (format-query-response query-params)
            :results (vec results)
            :count (count results)
            :template-used (:matched-template whitelist-check)}))
        (catch Exception e
          (log/error "Query execution error:" (.getMessage e) "Query:" (pr-str query-params))
          (common/error "Query execution failed" 500))))))
